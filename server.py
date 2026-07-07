"""
iperf3 Multi-Stream Server Monitor — Windows Compatible
Root cause fix: iperf3.exe block-buffers stdout saat di-pipe.
Solusi: jalankan iperf3 CLIENT (bukan server) dengan -J per-stream,
lalu server baca hasilnya lewat JSON yang di-flush tiap interval.

Karena kita adalah PENERIMA (server side), strategi terbaik di Windows:
Spawn iperf3 sebagai SERVER tanpa -J, tapi baca outputnya dengan
teknik pseudo-PTY menggunakan winpty — ATAU — gunakan pendekatan
yang lebih portabel: baca stderr+stdout via thread terpisah dengan
os.read() langsung (unbuffered syscall).
"""

import json
import subprocess
import threading
import time
import os
import re

# ── Global State ──────────────────────────────────────────────────────────────
stream_data      = {}
active_processes = []
data_lock        = threading.Lock()
START_TIME       = None


class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    RED    = "\033[31m"
    GREEN  = "\033[32m"
    YELLOW = "\033[33m"
    CYAN   = "\033[36m"
    GRAY   = "\033[90m"


def enable_ansi():
    if os.name == "nt":
        os.system("")  # trik agar cmd/Windows Terminal memproses escape ANSI


STATUS_COLOR = {
    "Standby" : C.YELLOW,
    "Running" : C.GREEN,
    "Finished": C.CYAN,
    "Error"   : C.RED,
}


def status_badge(status):
    label = f"{status.upper():^9}"
    color = STATUS_COLOR.get(status, C.GRAY)
    return f"{color}[{label}]{C.RESET}"


# ── Parsing Helpers ───────────────────────────────────────────────────────────

def is_interval_line(line):
    """True jika baris adalah data per-iterasi (bukan summary)."""
    m = re.search(r'(\d+\.\d+)-(\d+\.\d+)\s+sec', line)
    if not m:
        return False
    duration = float(m.group(2)) - float(m.group(1))
    # 0 < durasi <= 1.5: buang baris ekor durasi-nol (mis. "4.01-4.01 sec")
    return 0 < duration <= 1.5   # iterasi ~1 detik


def parse_text_line(line):
    """
    Parse baris teks iperf3 UDP server:
    [  5]  0.00-1.00 sec  567 KBytes  4.63 Mbits/sec  2.587 ms  2/403 (0.5%)
    Return (transfer_mb, bw_mbps, lost, total) atau None.
    """
    try:
        tm = re.search(r'sec\s+([\d.]+)\s+(K|M)Bytes', line, re.I)
        if not tm:
            return None
        transfer_mb = float(tm.group(1)) / 1024 if tm.group(2).upper() == 'K' \
                      else float(tm.group(1))

        # Terima Mbits/sec (normal, karena -f m) maupun Kbits/sec sebagai
        # jaring pengaman; keduanya dikonversi ke Mbps supaya konsisten.
        bm = re.search(r'([\d.]+)\s+(M|K)bits/sec', line)
        if not bm:
            return None
        bw_mbps = float(bm.group(1))
        if bm.group(2) == 'K':
            bw_mbps /= 1024

        dm = re.search(r'(\d+)/(\d+)\s*\(', line)
        lost  = int(dm.group(1)) if dm else 0
        total = int(dm.group(2)) if dm else 0

        return transfer_mb, bw_mbps, lost, total
    except Exception:
        return None


# ── Stats Update ──────────────────────────────────────────────────────────────

def reset_stats(title):
    with data_lock:
        stream_data[title]["stats"] = {
            "count": 0, "total_mb": 0.0,
            "sum_bw": 0.0, "total_lost": 0, "total_dgrams": 0,
        }
        stream_data[title]["latest"]  = "Koneksi baru masuk..."
        stream_data[title]["summary"] = "N/A"


def update_stats(title, transfer_mb, bw_mbps, lost, total, raw_line=""):
    with data_lock:
        st = stream_data[title]["stats"]
        st["count"]        += 1
        st["total_mb"]     += transfer_mb
        st["sum_bw"]       += bw_mbps
        st["total_lost"]   += lost
        st["total_dgrams"] += total

        # Baris summary dibangun di display_dashboard karena butuh proporsi
        # bandwidth antar-stream (tidak bisa dihitung per-worker).
        stream_data[title]["latest"] = raw_line or (
            f"BW: {bw_mbps:.2f} Mbps | "
            f"Transfer: {transfer_mb:.3f} MB | Lost: {lost}/{total}"
        )


# ── Pipe Reader (unbuffered, cross-platform) ──────────────────────────────────

def read_lines_unbuffered(pipe):
    """
    Baca pipe byte-per-byte menggunakan os.read() (syscall langsung).
    Ini BYPASS semua buffering Python maupun C-runtime iperf3.
    Bekerja di Windows maupun Linux.
    """
    buf = b""
    fd  = pipe.fileno()
    while True:
        try:
            ch = os.read(fd, 1)      # baca 1 byte — blocking, unbuffered
        except OSError:
            break
        if not ch:
            break
        if ch == b"\n":
            line = buf.decode("utf-8", errors="replace").rstrip("\r")
            buf  = b""
            if line:
                yield line
        else:
            buf += ch
    if buf:
        yield buf.decode("utf-8", errors="replace").rstrip("\r")


# ── Server Worker ─────────────────────────────────────────────────────────────

def run_iperf_server(stream_config):
    port  = stream_config["port"]
    title = stream_config["title"]

    stream_data[title] = {
        "latest" : "Listening...",
        "summary": "N/A",
        "status" : "Standby",
        "stats"  : {"count":0,"total_mb":0.0,"sum_bw":0.0,
                    "total_lost":0,"total_dgrams":0},
    }

    # "-f m" WAJIB: memaksa Bandwidth selalu Mbits/sec, sama seperti client.
    # Tanpa ini, interval di bawah 1 Mbps dicetak "Kbits/sec" dan gagal
    # di-parse -> interval terbuang -> total server jauh lebih kecil dari client.
    cmd = ["iperf3", "-s", "-p", str(port), "-i", "1", "-f", "m"]
    creation_flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0

    try:
        process = subprocess.Popen(
            cmd,
            stdout = subprocess.PIPE,
            stderr = subprocess.STDOUT,
            stdin  = subprocess.DEVNULL,
            text   = False,          # selalu bytes
            bufsize= 0,              # unbuffered
            creationflags=creation_flags,
        )
        active_processes.append(process)

        for line in read_lines_unbuffered(process.stdout):

            # ── Koneksi baru ─────────────────────────────────────────────
            if "connected" in line.lower():
                stream_data[title]["status"] = "Running"
                reset_stats(title)
                continue

            # ── Selesai ──────────────────────────────────────────────────
            if "iperf done" in line.lower():
                stream_data[title]["status"] = "Finished"
                continue

            # ── Data trafik ──────────────────────────────────────────────
            if "sec" in line and "Mbits/sec" in line:
                if not is_interval_line(line):
                    continue                      # lewati baris summary
                res = parse_text_line(line)
                if res:
                    update_stats(title, *res)

    except FileNotFoundError:
        stream_data[title]["latest"] = "ERROR: iperf3.exe tidak ditemukan di PATH!"
        stream_data[title]["status"] = "Error"
    except Exception as e:
        stream_data[title]["latest"] = f"Error: {e}"
        stream_data[title]["status"] = "Error"


# ── Dashboard ─────────────────────────────────────────────────────────────────

def display_dashboard(config):
    W = 100
    title_w = max(15, max(len(s['title']) for s in config['streams']) + 2)

    while True:
        os.system("cls" if os.name == "nt" else "clear")
        now = time.strftime("%H:%M:%S")
        elapsed = int(time.time() - START_TIME)
        mm, ss = divmod(elapsed, 60)

        print(C.CYAN + "=" * W + C.RESET)
        print(C.BOLD + f"{'IPERF3 MULTI-STREAM SERVER MONITOR':^{W}}" + C.RESET)
        print(f"{'Jam: ' + now + '   |   Waktu Aktif: ' + f'{mm:02d}:{ss:02d}':^{W}}")
        print(C.CYAN + "=" * W + C.RESET)

        print(C.BOLD + "\n  DATA TERBARU (LIVE — UPDATE TIAP ITERASI)" + C.RESET)
        print(C.GRAY + "  " + "-" * (W - 2) + C.RESET)
        with data_lock:
            for s in config["streams"]:
                t      = s["title"]
                d      = stream_data.get(t, {})
                status = d.get("status", "Standby")
                latest = d.get("latest", "")
                print(f"  {t:<{title_w}} {status_badge(status)}  {latest}")

            # Proporsi bandwidth: rata-rata tiap stream + totalnya,
            # supaya tiap stream tampil sebagai persentase dari total.
            avgs = {}
            for s in config["streams"]:
                st = stream_data.get(s["title"], {}).get("stats", {})
                c = st.get("count", 0)
                avgs[s["title"]] = (st["sum_bw"] / c) if c else 0.0
            grand_avg = sum(avgs.values())

            print(C.BOLD + "\n  SUMMARY AKUMULASI (DIHITUNG TIAP ITERASI)" + C.RESET)
            print(C.GRAY + "  " + "-" * (W - 2) + C.RESET)
            for s in config["streams"]:
                t  = s["title"]
                st = stream_data.get(t, {}).get("stats", {})
                if not st.get("count", 0):
                    print(f"  {C.BOLD}{t:<{title_w}}{C.RESET} N/A")
                    continue
                avg       = avgs[t]
                pct       = (avg / grand_avg * 100) if grand_avg else 0.0
                loss_rate = (st["total_lost"] / st["total_dgrams"] * 100) \
                            if st["total_dgrams"] > 0 else 0.0
                summary = (
                    f"Avg: {avg:.2f} Mbps ({pct:.1f}%/100%) | "
                    f"Total: {st['total_mb']:.2f} MB | "
                    f"Datagram Lost: {st['total_lost']}/{st['total_dgrams']} "
                    f"({loss_rate:.1f}%)"
                )
                print(f"  {C.BOLD}{t:<{title_w}}{C.RESET} {summary}")

        print(C.CYAN + "\n" + "=" * W + C.RESET)
        print(C.GRAY + "  Tekan Ctrl+C untuk berhenti." + C.RESET)
        time.sleep(1)


# ── Entry Point ───────────────────────────────────────────────────────────────

def main():
    global START_TIME
    enable_ansi()

    try:
        with open("server_config.json", "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        print("File server_config.json tidak ditemukan!")
        return
    except json.JSONDecodeError as e:
        print(f"Format server_config.json salah: {e}")
        return

    START_TIME = time.time()
    threading.Thread(target=display_dashboard, args=(config,), daemon=True).start()

    for s in config["streams"]:
        threading.Thread(target=run_iperf_server, args=(s,), daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for p in active_processes:
            p.terminate()
        print("\nSemua server dihentikan.")


if __name__ == "__main__":
    main()