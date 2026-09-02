import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app=FastAPI(); rooms={}; guard=asyncio.Lock()

@app.get('/')
async def health():return {'status':'ok','service':'SSServer Relay v7'}

@app.websocket('/ws')
async def relay(ws:WebSocket):
    await ws.accept(); pair_id=role=None; peer=None
    try:
        hello=json.loads(await asyncio.wait_for(ws.receive_text(),10)); pair_id=str(hello.get('pair_id','')); role=str(hello.get('role',''))
        if len(pair_id)!=32 or role not in ('server','viewer'):
            await ws.close(code=1008); return
        async with guard:
            room=rooms.setdefault(pair_id,{'event':asyncio.Event()})
            old=room.get(role)
            if old:
                try:await old.close(code=4001)
                except Exception:pass
            room[role]=ws
            if room.get('server') and room.get('viewer'):room['event'].set()
            event=room['event']
        await asyncio.wait_for(event.wait(),300)
        async with guard:
            room=rooms.get(pair_id,{})
            if room.get(role) is not ws:raise WebSocketDisconnect()
            peer=room.get('viewer' if role=='server' else 'server')
        if not peer:raise WebSocketDisconnect()
        await ws.send_text('READY')
        while True:
            data=await ws.receive_bytes(); await peer.send_bytes(data)
    except (WebSocketDisconnect,asyncio.TimeoutError,ValueError,json.JSONDecodeError):pass
    finally:
        async with guard:
            room=rooms.get(pair_id) if pair_id else None
            if room and room.get(role) is ws:
                room.pop(role,None); room['event'].clear()
                other=room.get('viewer' if role=='server' else 'server')
                if other:
                    try:await other.close(code=4002)
                    except Exception:pass
                if not room.get('server') and not room.get('viewer'):rooms.pop(pair_id,None)
