import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from bleak import BleakClient

HR_UUID = "00002a37-0000-1000-8000-00805f9b34fb"

clients: set[WebSocket] = set()
current_hr: int | None = None


async def broadcast(data: dict):
    dead = set()
    for ws in clients:
        try:
            await ws.send_json(data)
        except Exception:
            dead.add(ws)
    clients -= dead


async def hr_callback(sender, data: bytearray):
    global current_hr
    flags = data[0]
    hr = int.from_bytes(data[1:3], "little") if flags & 0x01 else data[1]
    current_hr = hr
    await broadcast({"hr": hr})


H10_ADDRESS = "24:AC:AC:08:40:DC"


async def ble_loop():
    print(f"Connecting to Polar H10 ({H10_ADDRESS})...")
    async with BleakClient(H10_ADDRESS) as client:
        await client.start_notify(HR_UUID, hr_callback)
        print("Streaming HR.")
        await asyncio.sleep(float("inf"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(ble_loop())
    yield
    task.cancel()
    try:
        await task
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
        await ws.send_json({"hr": current_hr})
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(ws)
