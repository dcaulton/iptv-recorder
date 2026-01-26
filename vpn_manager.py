from fastapi import FastAPI
from fastapi.responses import JSONResponse
from subprocess import Popen, PIPE
import sys
import glob
import os
import logging
import threading
import random
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

vpn_info = {}

app = FastAPI()


@app.get("/connect")
def connect(country: str):
    global vpn_info
    if vpn_info:
        err_msg = 'vpn already running, you need to shut it down first'
        logger.info(err_msg)
        return JSONResponse({"status": "failed", "message": err_msg})

    match_pattern = f"/configs/{country}*.nordvpn.com.udp.ovpn"
    files_arr = glob.glob(match_pattern)
    if not files_arr:
        err_msg = f'no ovpn endpoints found for country {country}'
        logger.info(err_msg)
        return JSONResponse({"status": "failed", "message": err_msg})

    ovpn_file = random.choice(files_arr) 
    process = Popen(["openvpn", "--config", ovpn_file, "--auth-user-pass", auth_file], stdout=PIPE, stderr=PIPE)
    threading.Thread(target=log_pipe, args=(process.stdout,)).start()
    threading.Thread(target=log_pipe, args=(process.stderr, "error")).start()
    logger.info(f'process pid is {process.pid}')
    vpn_info = {'country': country, 'pid': process.pid, 'ovpn_file': ovpn_file}
    return JSONResponse({"status": "started", "vpn_info": vpn_info})

def log_pipe(pipe, level="info"):
    for line in iter(pipe.readline, b''):
        logger.info(f"OpenVPN {level}: {line.decode().strip()}")  # Or use print()

@app.get("/disconnect")
def disconnect():
    global vpn_info
    if not vpn_info:
        return JSONResponse({"status": "not running"})
    os.kill(vpn_info.get('pid'), signal.SIGTERM)
    vpn_info = {}
    return JSONResponse({"status": "stopped"})

@app.get("/restart")
def restart(country: str):
    disconnect()
    return connect(country)

@app.get("/status")
def status():
    return JSONResponse(vpn_info)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
