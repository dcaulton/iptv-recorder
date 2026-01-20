from fastapi import FastAPI
from fastapi.responses import JSONResponse
from subprocess import Popen, PIPE
import os
import signal

app = FastAPI()

vpn_pids = {}  # country: PID

@app.get("/connect")
def connect(country: str):
    ovpn_file = f"/configs/{country}.ovpn"
    if not os.path.exists(ovpn_file):
        return JSONResponse({"status": "failed", "message": "No ovpn file for country"})
    if country in vpn_pids:
        return JSONResponse({"status": "already running"})
    process = Popen(["openvpn", "--config", ovpn_file], stdout=PIPE, stderr=PIPE)
    vpn_pids[country] = process.pid
    return JSONResponse({"status": "started", "pid": process.pid})

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
