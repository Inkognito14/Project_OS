#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."

PORT=5055
LOG_FILE="/tmp/test_server_$$.log"

cleanup() {
    if [[ -n "${SRV_PID:-}" ]] && kill -0 "$SRV_PID" 2>/dev/null; then
        kill -TERM "$SRV_PID" 2>/dev/null || true
        wait "$SRV_PID" 2>/dev/null || true
    fi
    rm -f "$LOG_FILE"
}
trap cleanup EXIT

echo "=== 1. Building server ==="
make clean && make

echo "=== 2. Starting ./server $PORT (FIFO policy) ==="
./server "$PORT" fifo > "$LOG_FILE" 2>&1 &
SRV_PID=$!
sleep 0.4

grep -q "server: listening on port $PORT" "$LOG_FILE"
echo "[PASS] Startup line printed"

echo "=== 3. Testing idle client non-blocking + 8 simultaneous clients ==="
python3 - "$PORT" << 'PYEOF'
import socket, sys, threading, time

port = int(sys.argv[1])

# Open an idle client that connects and sends nothing
idle = socket.create_connection(("127.0.0.1", port), timeout=5)

results = [None] * 8
errors = []

def submit_client(idx):
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=5)
        s.sendall(f"SUBMIT 10 {idx % 10} echo client-{idx}\n".encode())
        resp = s.makefile("r").readline().strip()
        s.close()
        results[idx] = resp
    except Exception as e:
        errors.append(str(e))

t0 = time.time()
threads = [threading.Thread(target=submit_client, args=(i,)) for i in range(8)]
for t in threads: t.start()
for t in threads: t.join()
elapsed = time.time() - t0

idle.close()

assert not errors, f"Client errors: {errors}"
assert len(set(results)) == 8, f"Expected 8 unique IDs, got {results}"
assert elapsed < 2.0, f"Too slow ({elapsed:.2f}s) — idle client may have blocked!"
print(f"[PASS] 8 simultaneous clients got 8 unique IDs in {elapsed:.3f}s while idle client held socket: {results}")
PYEOF

echo "=== 4. Testing FIFO dispatch order with mock worker ==="
python3 - "$PORT" << 'PYEOF'
import socket, sys

port = int(sys.argv[1])
w = socket.create_connection(("127.0.0.1", port), timeout=5)
wf = w.makefile("r")
w.sendall(b"WORKER 1\n")
ok = wf.readline().strip()
assert ok == "OK", f"Expected OK, got {ok}"

dispatched = []
for _ in range(8):
    msg = wf.readline().strip()
    parts = msg.split()
    assert parts[0] == "RUN", f"Expected RUN, got {msg}"
    job_id = parts[1]
    dispatched.append(job_id)
    w.sendall(f"REPORT {job_id} DONE 0 3\nhi\n".encode())

wf.close()
w.close()
seqs = [int(x.split("-")[1]) for x in dispatched]
assert seqs == sorted(seqs), f"FIFO order violated: {dispatched}"
print(f"[PASS] FIFO dispatched in exact arrival order: {dispatched}")
PYEOF

echo "=== 5. Testing SIGTERM clean shutdown and immediate restart ==="
kill -TERM "$SRV_PID"
wait "$SRV_PID"
SRV_PID=""

./server "$PORT" priority > "$LOG_FILE" 2>&1 &
SRV_PID=$!
sleep 0.4
grep -q "server: listening on port $PORT" "$LOG_FILE"
echo "[PASS] Immediate restart after SIGTERM succeeded (SO_REUSEADDR)"

echo "=== 6. Testing Priority-with-Aging dispatch order ==="
python3 - "$PORT" << 'PYEOF'
import socket, sys

port = int(sys.argv[1])

def send_cmd(cmd):
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.sendall((cmd + "\n").encode())
    resp = s.makefile("r").readline().strip()
    s.close()
    return resp

# Submit low-priority (0), medium (5), and high (9) jobs before worker connects
id_low  = send_cmd("SUBMIT 10 0 echo low")
id_med  = send_cmd("SUBMIT 10 5 echo med")
id_high = send_cmd("SUBMIT 10 9 echo high")

w = socket.create_connection(("127.0.0.1", port), timeout=5)
wf = w.makefile("r")
w.sendall(b"WORKER 1\n")
assert wf.readline().strip() == "OK"

order = []
for _ in range(3):
    parts = wf.readline().strip().split()
    order.append(parts[1])
    w.sendall(f"REPORT {parts[1]} DONE 0 0\n".encode())
wf.close()
w.close()

assert order == [id_high, id_med, id_low], f"Expected priority order {[id_high, id_med, id_low]}, got {order}"
print(f"[PASS] Priority-with-Aging dispatched highest priority first: {order}")
PYEOF

echo "=== 7. Checking FD leak across 50 requests ==="
FD_BEFORE=$(ls /proc/"$SRV_PID"/fd | wc -l)
python3 - "$PORT" << 'PYEOF'
import socket, sys
port = int(sys.argv[1])
for _ in range(50):
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.sendall(b"STATUS T1-99999\n")
    resp = s.makefile("r").readline().strip()
    assert resp == "T1-99999 UNKNOWN", f"Unexpected: {resp}"
    s.close()
PYEOF
sleep 0.3
FD_AFTER=$(ls /proc/"$SRV_PID"/fd | wc -l)
echo "Open FDs before=$FD_BEFORE after=$FD_AFTER"
test "$FD_BEFORE" -eq "$FD_AFTER"
echo "[PASS] Zero leaked file descriptors!"

echo "=== ALL TESTS PASSED ==="
