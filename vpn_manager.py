from fastapi import FastAPI
from fastapi.responses import JSONResponse
from subprocess import Popen, PIPE
import os
import threading
import signal

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout,  # explicit stdout (Docker captures this)
    force=True          # override any existing config
)
logger = logging.getLogger(__name__)
logger.info(f"just waking up in the logger")

auth_file = "/tmp/auth.txt"
with open(auth_file, "w") as f:
    f.write(f"{os.getenv('NORD_USERNAME')}\n{os.getenv('NORD_PASSWORD')}\n")
logger.info(f"auth file written")

ovpn_file = f"/configs/uk1699.nordvpn.com.udp.ovpn"

app = FastAPI()

vpn_pids = {}  # country: PID

@app.get("/connect")
def connect(country: str):
#    ovpn_file = f"/configs/{country}.ovpn"
    logger.info('opening vpn file')
    if not os.path.exists(ovpn_file):
        logger.info('no file found')
        return JSONResponse({"status": "failed", "message": "No ovpn file for country"})
    if country in vpn_pids:
        logger.info('country already running')
        return JSONResponse({"status": "already running"})
    process = Popen(["openvpn", "--config", ovpn_file, "--auth-user-pass", auth_file], stdout=PIPE, stderr=PIPE)
    threading.Thread(target=log_pipe, args=(process.stdout,)).start()
    threading.Thread(target=log_pipe, args=(process.stderr, "error")).start()
    logger.info(f'process pid is {process.pid}')
    vpn_pids[country] = process.pid
    return JSONResponse({"status": "started", "pid": process.pid})

def log_pipe(pipe, level="info"):
    for line in iter(pipe.readline, b''):
        logger.info(f"OpenVPN {level}: {line.decode().strip()}")  # Or use print()

@app.get("/disconnect")
def disconnect(country: str):
    if country not in vpn_pids:
        return JSONResponse({"status": "not running"})
    os.kill(vpn_pids[country], signal.SIGTERM)
    del vpn_pids[country]
    return JSONResponse({"status": "stopped"})

@app.get("/restart")
def restart(country: str):
    disconnect(country)
    return connect(country)

@app.get("/status")
def status():
    return JSONResponse(vpn_pids)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
