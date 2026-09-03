import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI(); rooms = {}; v8_rooms = {}; guard = asyncio.Lock()

@app.get('/')
async def health(): return {'status': 'ok', 'service': 'SSServer Relay', 'versions': ['v7', 'v8']}

@app.websocket('/ws')
async def relay_v7(ws: WebSocket):
    """Keep the V7 endpoint alive while the user migrates to V8."""
    await ws.accept(); pair_id = role = None; peer = None
    try:
        hello = json.loads(await asyncio.wait_for(ws.receive_text(), 10)); pair_id = str(hello.get('pair_id', '')); role = str(hello.get('role', ''))
        if len(pair_id) != 32 or role not in ('server', 'viewer'): await ws.close(code=1008); return
        async with guard:
            room = rooms.setdefault(pair_id, {'event': asyncio.Event()}); old = room.get(role)
            if old:
                try: await old.close(code=4001)
                except Exception: pass
            room[role] = ws
            if room.get('server') and room.get('viewer'): room['event'].set()
            event = room['event']
        await asyncio.wait_for(event.wait(), 300)
        async with guard:
            room = rooms.get(pair_id, {})
            if room.get(role) is not ws: raise WebSocketDisconnect()
            peer = room.get('viewer' if role == 'server' else 'server')
        if not peer: raise WebSocketDisconnect()
        await ws.send_text('READY')
        while True:
            data = await ws.receive_bytes(); await peer.send_bytes(data)
    except (WebSocketDisconnect, asyncio.TimeoutError, ValueError, json.JSONDecodeError): pass
    finally:
        async with guard:
            room = rooms.get(pair_id) if pair_id else None
            if room and room.get(role) is ws:
                room.pop(role, None); room['event'].clear(); other = room.get('viewer' if role == 'server' else 'server')
                if other:
                    try: await other.close(code=4002)
                    except Exception: pass
                if not room.get('server') and not room.get('viewer'): rooms.pop(pair_id, None)

@app.websocket('/ws/v8')
async def relay_v8(ws: WebSocket):
    await ws.accept(); pair_id = role = device_id = ''; peer = None
    try:
        hello = json.loads(await asyncio.wait_for(ws.receive_text(), 10)); pair_id = str(hello.get('pair_id', '')); role = str(hello.get('role', '')); device_id = str(hello.get('device_id', ''))
        if len(pair_id) != 32 or role not in ('server', 'viewer', 'list'): await ws.close(code=1008); return
        if role == 'list':
            async with guard:
                servers = v8_rooms.get(pair_id, {}).get('servers', {})
                devices = [{'device_id': k, 'device_name': v['name'], 'busy': bool(v.get('viewer'))} for k, v in servers.items()]
            await ws.send_text(json.dumps({'type': 'devices', 'devices': devices}, ensure_ascii=False)); return
        if len(device_id) != 32: await ws.send_text('ERROR:잘못된 장치 ID'); return
        if role == 'server':
            name = str(hello.get('device_name', 'Windows PC'))[:80]
            async with guard:
                room = v8_rooms.setdefault(pair_id, {'servers': {}}); old = room['servers'].get(device_id)
                if old:
                    try: await old['ws'].close(code=4101)
                    except Exception: pass
                entry = {'ws': ws, 'name': name, 'viewer': None, 'event': asyncio.Event()}; room['servers'][device_id] = entry
            await ws.send_text('WAIT'); await asyncio.wait_for(entry['event'].wait(), 86400)
            async with guard:
                current = v8_rooms.get(pair_id, {}).get('servers', {}).get(device_id)
                if current is not entry or not entry.get('viewer'): raise WebSocketDisconnect()
                peer = entry['viewer']
            await ws.send_text('READY')
        else:
            async with guard:
                entry = v8_rooms.get(pair_id, {}).get('servers', {}).get(device_id)
                if not entry: await ws.send_text('ERROR:선택한 PC가 오프라인입니다.'); return
                if entry.get('viewer'): await ws.send_text('ERROR:선택한 PC는 이미 사용 중입니다.'); return
                entry['viewer'] = ws; peer = entry['ws']; entry['event'].set()
            await ws.send_text('READY')
        while True:
            data = await ws.receive_bytes(); await peer.send_bytes(data)
    except (WebSocketDisconnect, asyncio.TimeoutError, ValueError, json.JSONDecodeError): pass
    finally:
        async with guard:
            room = v8_rooms.get(pair_id) if pair_id else None; entry = room.get('servers', {}).get(device_id) if room else None
            if entry:
                if role == 'server' and entry.get('ws') is ws:
                    room['servers'].pop(device_id, None); other = entry.get('viewer')
                    if other:
                        try: await other.close(code=4102)
                        except Exception: pass
                elif role == 'viewer' and entry.get('viewer') is ws:
                    entry['viewer'] = None
                    try: await entry['ws'].close(code=4103)
                    except Exception: pass
                    room['servers'].pop(device_id, None)
            if room and not room.get('servers'): v8_rooms.pop(pair_id, None)
