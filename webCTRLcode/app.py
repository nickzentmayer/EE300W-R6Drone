import json
import time
import threading
import subprocess
from pathlib import Path
from flask import Flask, Response, render_template, jsonify, request
import sys

app = Flask(__name__)

@app.errorhandler(404)
def not_found(e):
    from flask import request as req
    print(f"[404] {req.method} {req.path}")
    return jsonify({"error": "not found", "path": req.path, "method": req.method, "detail": str(e)}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "server error", "detail": str(e)}), 500


# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).parent.resolve()
CODES_FILE     = BASE_DIR / "ir_codes.json"
RECORDINGS_DIR = BASE_DIR / "recordings"
LIRC_RX        = "/dev/lirc1"   # IR receiver (gpio-ir,  pin 17)
LIRC_TX        = "/dev/lirc0"   # IR transmitter (gpio-ir-tx, pin 26)
CAM_ROTATION   = 0                 # degrees: 0, 90, 180, 270
RECORDINGS_DIR.mkdir(exist_ok=True)

# ── IR state ──────────────────────────────────────────────────────────────────
# ir_codes maps name -> path of the saved .txt timing file
ir_codes: dict = json.loads(CODES_FILE.read_text()) if CODES_FILE.exists() else {}
_recording     = False
_record_name   = ""
_record_proc   = None          # the live ir-ctl subprocess
_record_lock   = threading.Lock()


def save_codes():
    CODES_FILE.write_text(json.dumps(ir_codes, indent=2))


# ── Camera broadcaster ───────────────────────────────────────────────────────
import queue

_cam_clients: list = []
_cam_lock    = threading.Lock()
_cam_thread  = None
_cam_proc    = None   # the live rpicam-vid process
killme = False

def _camera_broadcaster():
    """Single rpicam-vid process; fans frames out to all connected clients."""
    global killme
    cmd = [
        "rpicam-vid",
        "--inline",
        "--nopreview",
        "--codec", "mjpeg",
        "--width",  "1280",
        "--height", "720",
        "--framerate", "30",
        "--timeout", "0",
        #"--rotation", "180"   
    ]
    if CAM_ROTATION == 90:
        cmd += ["--hflip", "1", "--vflip", "1", "--rotation", "180"]  # 90 = hflip+vflip+180
    elif CAM_ROTATION == 180:
        cmd += ["--rotation", "180"]
    elif CAM_ROTATION == 270:
        cmd += ["--hflip", "1", "--vflip", "1"]  # 270 = hflip+vflip
    cmd += ["-o", "-"]
    print(cmd, file=sys.stderr)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    
    buf = b""
    try:
        while True:
            if killme:
                proc.terminate()
                killme = False
                return
            chunk = proc.stdout.read(2048)
            if not chunk:
                break
            buf += chunk
            start = buf.find(b"\xff\xd8")
            end   = buf.find(b"\xff\xd9")
            if start != -1 and end != -1 and end > start:
                frame = buf[start:end + 2]
                buf   = buf[end + 2:]
                packet = (
                    b"--FRAME\r\n"
                    + b"Content-Type: image/jpeg\r\n"
                    + f"Content-Length: {len(frame)}\r\n\r\n".encode()
                    + frame
                    + b"\r\n"
                )
                with _cam_lock:
                    for q in _cam_clients:
                        try:
                            q.put_nowait(packet)
                        except queue.Full:
                            pass   # slow client — drop frame
    finally:
        proc.terminate()
 
 
def _ensure_camera():
    """Start the broadcaster thread if it isn't running."""
    global _cam_thread
    if _cam_thread is None or not _cam_thread.is_alive():
        _cam_thread = threading.Thread(target=_camera_broadcaster, daemon=True)
        _cam_thread.start()
 
 
def gen_frames():
    """Per-client generator: registers a queue, yields frames, then cleans up."""
    while True:
        _ensure_camera()
        q = queue.Queue(maxsize=5)
        with _cam_lock:
            _cam_clients.append(q)
        try:
            while True:
                packet = q.get(timeout=10)
                if packet is None:
                    # Sentinel — rotation changed, restart broadcaster
                    break
                yield packet
        except queue.Empty:
            return   # client disconnected
        finally:
            with _cam_lock:
                try:
                    _cam_clients.remove(q)
                except ValueError:
                    pass
        # Small pause to let old broadcaster die before restarting
        time.sleep(0.5)
 
 
@app.route("/video_feed")
def video_feed():
    return Response(
        gen_frames(),
        mimetype="multipart/x-mixed-replace; boundary=FRAME"
    )
 
 
# ── IR record ─────────────────────────────────────────────────────────────────
def _record_worker(name: str):
    """Run ir-ctl --receive and kill it once a signal arrives or stop is called."""
    global _recording, _record_proc
 
    out_file = RECORDINGS_DIR / f"{name}.txt"
    cmd = ["ir-ctl", "-d", LIRC_RX, f"--receive={out_file}"]
 
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        with _record_lock:
            _record_proc = proc
 
        deadline = time.time() + 15   # max 15 s window
        while _recording and time.time() < deadline:
            # Check if the file has been written to (signal received)
            if out_file.exists() and out_file.stat().st_size > 0:
                time.sleep(0.3)        # let ir-ctl finish flushing
                break
            time.sleep(0.1)
 
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
 
        if out_file.exists() and out_file.stat().st_size > 0:
            ir_codes[name] = str(out_file)
            save_codes()
            print(f"[IR] Recorded '{name}' -> {out_file}")
        else:
            print(f"[IR] Nothing captured for '{name}'")
            if out_file.exists():
                out_file.unlink()
 
    except Exception as e:
        print(f"[IR] Record error: {e}")
    finally:
        with _record_lock:
            _recording    = False
            _record_proc  = None
 
 
# ── IR transmit ───────────────────────────────────────────────────────────────
def _transmit_worker(name: str):
    """Send a saved IR timing file via ir-ctl."""
    stored = ir_codes[name]
    file_path = Path(stored)
 
    # If the stored path doesn't exist, try resolving just the filename
    # relative to RECORDINGS_DIR (handles moved/renamed app directory)
    if not file_path.exists():
        fallback = RECORDINGS_DIR / file_path.name
        if fallback.exists():
            print(f"[TX] Stored path missing, using fallback: {fallback}")
            file_path = fallback
            # Update the stored path so it's correct going forward
            ir_codes[name] = str(file_path)
            save_codes()
        else:
            print(f"[TX] File not found: {file_path} (also tried {fallback})")
            return
    try:
        result = subprocess.run(
            ["ir-ctl", "-d", LIRC_TX, f"--send={file_path}"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            print(f"[TX] Sent '{name}'")
        else:
            print(f"[TX] ir-ctl error: {result.stderr.strip()}")
    except Exception as e:
        print(f"[TX] Error: {e}")
 
 
# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")
 
 
@app.route("/api/ir/codes")
def api_list_codes():
    return jsonify(list(ir_codes.keys()))
 
 
@app.route("/api/ir/record", methods=["POST"])
def api_record():
    global _recording, _record_name
    name = request.json.get("name", "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    if _recording:
        return jsonify({"error": "already recording"}), 409
    _recording   = True
    _record_name = name
    t = threading.Thread(target=_record_worker, args=(name,), daemon=True)
    t.start()
    return jsonify({"status": "recording", "name": name})
 
 
@app.route("/api/ir/record/status")
def api_record_status():
    return jsonify({"recording": _recording, "name": _record_name})
 
 
@app.route("/api/ir/record/stop", methods=["POST"])
def api_record_stop():
    global _recording, _record_proc
    _recording = False
    with _record_lock:
        if _record_proc:
            _record_proc.terminate()
    return jsonify({"status": "stopped"})
 
 
@app.route("/api/ir/send/<name>", methods=["POST"])
def api_send(name):
    if name not in ir_codes:
        return jsonify({"error": "code not found"}), 404
    t = threading.Thread(target=_transmit_worker, args=(name,), daemon=True)
    t.start()
    return jsonify({"status": "sent", "name": name})
 
 
@app.route("/api/ir/delete/<name>", methods=["POST"])
def api_delete(name):
    if name not in ir_codes:
        return jsonify({"error": "not found"}), 404
    # Remove the timing file too
    file_path = Path(ir_codes[name])
    if file_path.exists():
        file_path.unlink()
    del ir_codes[name]
    save_codes()
    return jsonify({"status": "deleted", "name": name})
 
 
@app.route("/api/ir/rename", methods=["POST"])
def api_rename():
    old = request.json.get("old", "").strip()
    new = request.json.get("new", "").strip()
    if old not in ir_codes:
        return jsonify({"error": "not found"}), 404
    if not new:
        return jsonify({"error": "new name required"}), 400
    # Rename the timing file on disk too
    old_path = Path(ir_codes[old])
    new_path = RECORDINGS_DIR / f"{new}.txt"
    if old_path.exists():
        old_path.rename(new_path)
    ir_codes[new] = str(new_path)
    del ir_codes[old]
    save_codes()
    return jsonify({"status": "renamed"})
 
 
 
@app.route("/api/camera/rotation")
def api_get_rotation():
    return jsonify({"rotation": CAM_ROTATION})
 
 
@app.route("/api/camera/rotation", methods=["POST"])
def api_set_rotation():
    global CAM_ROTATION, _cam_thread, _cam_proc
    global killme
    deg = request.json.get("rotation", 0)
    if deg not in (0, 90, 180, 270):
        return jsonify({"error": "rotation must be 0, 90, 180 or 270"}), 400
    CAM_ROTATION = deg
    #save_config({"cam_rotation": deg})
    # Kill the rpicam-vid process directly so it restarts with new rotation
    print(f"[CAM] Rotation set to {CAM_ROTATION}°, restarting camera...")
    with _cam_lock:
        killme = True
        # Send sentinel to all clients so gen_frames loops and restarts
        for q in _cam_clients:
            try:
                q.put_nowait(None)
            except Exception:
                pass
    return jsonify({"rotation": CAM_ROTATION})
 
 
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)