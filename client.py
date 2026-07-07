import json
import subprocess
import threading
import time
import os
import sys
import re

stream_data = {}
START_TIME = None


class C:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"
    GRAY = "\033[90m"


def enable_ansi():
    if os.name == "nt":
        os.system("")  # trik agar cmd/Windows Terminal memproses escape ANSI


STATUS_LABEL = {
    "Starting": "STARTING",
    "Running": "RUNNING",
    "Finished": "FINISHED",
    "Failed": "FAILED",
    "Error System": "ERROR",
}

STATUS_COLOR = {
    "Starting": C.YELLOW,
    "Running": C.GREEN,
    "Finished": C.CYAN,
    "Failed": C.RED,
    "Error System": C.RED,
}


def status_badge(status):
    label = f"{STATUS_LABEL.get(status, status.upper()):^10}"
    color = STATUS_COLOR.get(status, C.GRAY)
    return f"{color}[{label}]{C.RESET}"

def read_lines_unbuffered(pipe):
    """
    Baca pipe byte-per-byte lewat os.read() (syscall langsung), bypass
    buffering iperf3.exe sendiri saat stdout-nya di-pipe (bukan TTY),
    supaya baris data langsung sampai per-iterasi (realtime).
    """
    buf = b""
    fd = pipe.fileno()
    while True:
        try:
            ch = os.read(fd, 1)
        except OSError:
            break
        if not ch:
            break
        if ch == b"\n":
            line = buf.decode("utf-8", errors="replace").rstrip("\r")
            buf = b""
            if line:
                yield line
        else:
            buf += ch
    if buf:
        yield buf.decode("utf-8", errors="replace").rstrip("\r")


def is_interval_line(line):
    """True hanya untuk baris per-iterasi (bukan baris summary akhir test)."""
    m = re.search(r'(\d+\.\d+)-(\d+\.\d+)\s+sec', line)
    if not m:
        return False
    duration = float(m.group(2)) - float(m.group(1))
    return duration <= 1.5


def parse_iperf_line(line):
    tokens = line.split()
    try:
        bitrate_idx = -1
        for i, t in enumerate(tokens):
            if "bits/sec" in t.lower():
                bitrate_idx = i
                break

        if bitrate_idx > 0:
            bitrate = float(tokens[bitrate_idx-1])
            transfer_val = float(tokens[bitrate_idx-3])
            transfer_unit = tokens[bitrate_idx-2].lower()

            # "-f m" cuma memaksa unit Bandwidth ke Mbits/sec; kolom Transfer
            # tetap auto-scale (Bytes/KBytes/MBytes/GBytes) mengikuti besarannya.
            if transfer_unit.startswith("k"):
                transfer = transfer_val / 1024
            elif transfer_unit.startswith("g"):
                transfer = transfer_val * 1024
            elif transfer_unit.startswith("m"):
                transfer = transfer_val
            else:  # Bytes polos
                transfer = transfer_val / (1024 * 1024)

            loss, total = 0, 0
            for t in tokens[bitrate_idx+1:]:
                if '/' in t:
                    parts = t.split('/')
                    loss = int(parts[0])
                    total = int(parts[1])
                    break
                elif t.isdigit(): # Format Client umumnya hanya menampilkan total sent
                    total = int(t)
                    break
                    
            return transfer, bitrate, loss, total
    except:
        pass
    return None

def run_iperf_client(server_ip, stream_config):
    port = stream_config['port']
    title = stream_config['title']
    bw = stream_config['bandwidth']
    tos = stream_config['tos']
    duration = stream_config['time']
    
    stream_data[title] = {
        "latest": "Menunggu koneksi...",
        "summary": "Menghitung...",
        "status": "Running",
        "stats": {"count": 0, "sum_transfer": 0.0, "sum_bitrate": 0.0, "sum_total_dgrams": 0}
    }

    cmd = [
        "iperf3", "-c", server_ip, 
        "-p", str(port), 
        "-u", 
        "-b", bw, 
        "-t", str(duration), 
        "--tos", str(tos),
        "-f", "m",          # Memaksa format ke Mbits/MBytes agar seragam
    ]

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            text=False,   # baca sebagai bytes untuk read_lines_unbuffered
            bufsize=0,    # unbuffered di sisi Python
        )

        for line in read_lines_unbuffered(process.stdout):
            line = line.strip()
            if not line:
                continue
            
            if "error" in line.lower() or "refused" in line.lower() or "denied" in line.lower():
                stream_data[title]["latest"] = f"ERROR: {line}"
                stream_data[title]["status"] = "Failed"
                continue 

            if "sender" in line.lower() or "receiver" in line.lower():
                continue # Abaikan baris summary bawaan iperf, kita pakai hitungan manual

            if "sec" in line and "Bytes" in line and "bits/sec" in line:
                if not is_interval_line(line):
                    continue  # baris summary akhir (nilai kumulatif), bukan per-iterasi

                parsed = parse_iperf_line(line)

                if parsed:
                    transfer, bitrate, loss, total = parsed
                    stats = stream_data[title]["stats"]

                    stats["count"] += 1
                    stats["sum_transfer"] += transfer
                    stats["sum_bitrate"] += bitrate
                    stats["sum_total_dgrams"] += total

                    avg_bitrate = stats["sum_bitrate"] / stats["count"]

                    stream_data[title]["latest"] = (
                        f"Transfer: {transfer:7.2f} MB | Bitrate: {bitrate:8.2f} Mbps | Datagram: {total}"
                    )
                    stream_data[title]["summary"] = (
                        f"Rata-rata: {avg_bitrate:.2f} Mbps | "
                        f"Total Transfer: {stats['sum_transfer']:.2f} MB | "
                        f"Datagram Sent: {stats['sum_total_dgrams']}"
                    )
                else:
                    stream_data[title]["latest"] = line

        process.wait()
        
        if stream_data[title]["status"] == "Running":
            stream_data[title]["status"] = "Finished"

    except Exception as e:
        stream_data[title]["status"] = "Error System"
        stream_data[title]["latest"] = str(e)

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def display_dashboard(config):
    W = 96
    title_w = max(15, max(len(s['title']) for s in config['streams']) + 2)

    while True:
        clear_screen()
        elapsed = int(time.time() - START_TIME)
        mm, ss = divmod(elapsed, 60)

        print(C.CYAN + "=" * W + C.RESET)
        print(C.BOLD + f"{'IPERF3 MULTI-STREAM CLIENT DASHBOARD':^{W}}" + C.RESET)
        print(f"{'Target Server: ' + config['server_ip'] + '   |   Waktu Berjalan: ' + f'{mm:02d}:{ss:02d}':^{W}}")
        print(C.CYAN + "=" * W + C.RESET)

        print(C.BOLD + "\n  DATA TERKINI (LIVE)" + C.RESET)
        print(C.GRAY + "  " + "-" * (W - 2) + C.RESET)
        all_finished = True
        for stream in config['streams']:
            title = stream['title']
            data = stream_data.get(title, {})
            status = data.get("status", "Starting")
            latest = data.get("latest", "...")
            print(f"  {title:<{title_w}} {status_badge(status)}  {latest}")

            if status == "Running" or status == "Starting":
                all_finished = False

        print(C.BOLD + "\n  RINGKASAN (RATA-RATA, TOTAL TRANSFER & DATAGRAM)" + C.RESET)
        print(C.GRAY + "  " + "-" * (W - 2) + C.RESET)
        for stream in config['streams']:
            title = stream['title']
            data = stream_data.get(title, {})
            summary = data.get("summary", "Menunggu pengujian selesai...")
            print(f"  {C.BOLD}{title:<{title_w}}{C.RESET} {summary}")

        print(C.CYAN + "\n" + "=" * W + C.RESET)
        if all_finished:
            print(C.GREEN + C.BOLD + "  [OK] Pengujian Selesai." + C.RESET)
            break
        else:
            print(C.GRAY + "  Tekan Ctrl+C untuk berhenti." + C.RESET)

        time.sleep(1)

def main():
    global START_TIME
    enable_ansi()

    try:
        with open('client_config.json', 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print("File client_config.json tidak ditemukan!")
        sys.exit(1)

    server_ip = config['server_ip']
    threads = []
    START_TIME = time.time()

    dash_thread = threading.Thread(target=display_dashboard, args=(config,), daemon=True)
    dash_thread.start()

    for stream in config['streams']:
        t = threading.Thread(target=run_iperf_client, args=(server_ip, stream))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
        
    time.sleep(1.5)

if __name__ == "__main__":
    main()