import asyncio
import math
from collections import deque
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from bleak import BleakClient

HR_UUID   = "00002a37-0000-1000-8000-00805f9b34fb"
PMD_CP    = "fb005c81-02e7-f387-1cad-8acd2d8df0c8"
PMD_DATA  = "fb005c82-02e7-f387-1cad-8acd2d8df0c8"

H10_ADDRESS = "24:AC:AC:08:40:DC"
RHR = 85

# START_MEASUREMENT | ECG | SAMPLE_RATE=130Hz | RESOLUTION=14bit
ECG_START = bytearray([0x02, 0x00, 0x00, 0x01, 0x82, 0x00, 0x01, 0x01, 0x0E, 0x00])

clients: set[WebSocket] = set()
rr_buffer: deque[float] = deque(maxlen=30)
ecg_batch: list[int] = []

current_hr: int | None = None
current_rmssd: float | None = None
current_stress: int | None = None


def calc_rmssd(rr: list[float]) -> float:
    diffs = [rr[i + 1] - rr[i] for i in range(len(rr) - 1)]
    return math.sqrt(sum(d * d for d in diffs) / len(diffs))


def calc_stress(hr: int, rmssd: float) -> int:
    hrv_stress = max(0.0, min(100.0, (80 - rmssd) / 65 * 100))
    hr_stress  = max(0.0, min(100.0, (hr - RHR) / RHR * 200))
    return round(hrv_stress * 0.65 + hr_stress * 0.35)


async def broadcast(data: dict):
    dead = set()
    for ws in clients:
        try:
            await ws.send_json(data)
        except Exception:
            dead.add(ws)
    clients.difference_update(dead)


async def hr_callback(sender, data: bytearray):
    global current_hr, current_rmssd, current_stress

    flags  = data[0]
    hr     = int.from_bytes(data[1:3], "little") if flags & 0x01 else data[1]
    offset = 3 if flags & 0x01 else 2

    if flags & 0x08:
        offset += 2

    if flags & 0x10:
        while offset + 1 < len(data):
            rr_raw = int.from_bytes(data[offset:offset + 2], "little")
            rr_buffer.append(rr_raw * 1000 / 1024)
            offset += 2

    rmssd  = calc_rmssd(list(rr_buffer)) if len(rr_buffer) >= 2 else None
    stress = calc_stress(hr, rmssd) if rmssd is not None else None

    current_hr, current_rmssd, current_stress = hr, rmssd, stress

    payload: dict = {"type": "hr", "hr": hr}
    if rmssd is not None:
        payload["rmssd"]  = round(rmssd, 1)
        payload["stress"] = stress
    await broadcast(payload)


async def ecg_callback(sender, data: bytearray):
    # byte 0: measurement type (0x00 = ECG)
    # bytes 1-8: timestamp ns uint64 LE
    # byte 9: frame type
    # bytes 10+: samples, 3 bytes each, signed LE
    if data[0] != 0x00 or len(data) < 11:
        return
    offset = 10
    while offset + 2 < len(data):
        sample = int.from_bytes(data[offset:offset + 3], "little", signed=True)
        ecg_batch.append(sample)
        offset += 3


async def pmd_cp_callback(sender, data: bytearray):
    if len(data) >= 3 and data[0] == 0x02 and data[1] == 0x00:
        err = data[2]
        print("ECG started." if err == 0x00 else f"ECG start error 0x{err:02x}")


async def ecg_sender():
    global ecg_batch
    while True:
        await asyncio.sleep(0.05)
        if ecg_batch:
            batch     = ecg_batch
            ecg_batch = []
            await broadcast({"type": "ecg", "samples": batch})


async def ble_loop():
    print(f"Connecting to Polar H10 ({H10_ADDRESS})...")
    async with BleakClient(H10_ADDRESS) as client:
        await client.start_notify(HR_UUID,  hr_callback)
        await client.start_notify(PMD_CP,   pmd_cp_callback)
        await client.start_notify(PMD_DATA, ecg_callback)
        await client.write_gatt_char(PMD_CP, ECG_START, response=True)
        print("Streaming HR + ECG.")
        await asyncio.sleep(float("inf"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    ble_task  = asyncio.create_task(ble_loop())
    send_task = asyncio.create_task(ecg_sender())
    yield
    ble_task.cancel()
    send_task.cancel()
    for t in (ble_task, send_task):
        try:
            await t
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def index():
    return FileResponse("static/index.html")


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    if current_hr is not None:
        payload: dict = {"type": "hr", "hr": current_hr}
        if current_rmssd is not None:
            payload["rmssd"]  = round(current_rmssd, 1)
            payload["stress"] = current_stress
        await ws.send_json(payload)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(ws)
