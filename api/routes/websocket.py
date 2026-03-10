from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import asyncio
import json
from services.redis_service import redis_db

router = APIRouter()


@router.websocket("/ws/live")
async def websocket_live_endpoint(websocket: WebSocket):
    await websocket.accept()
    
    redis = redis_db.get_client()
    pubsub = redis.pubsub()
    await pubsub.subscribe("metrics-update")

    async def get_current_metrics():
        summary = await redis.hgetall("metric:summary")
        # Ensure we have some structure even if redis is empty
        top_clicks = await redis.zrange("top:products:click", 0, 4, desc=True, withscores=True)
        return {
            "type": "metrics_update",
            "data": summary or {},
            "top_products": [{"product_id": item[0], "score": item[1]} for item in top_clicks]
        }

    # Send initial data immediately
    initial_data = await get_current_metrics()
    await websocket.send_json(initial_data)

    async def reader(ws: WebSocket):
        try:
            while True:
                # Keep connection alive, we don't expect messages from client yet
                await ws.receive_text()
        except WebSocketDisconnect:
            pass

    async def sender(ws: WebSocket):
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    # When Spark notifies of a refresh, we pull latest metrics and push
                    full_data = await get_current_metrics()
                    await ws.send_json(full_data)
        except Exception as e:
            print(f"WS Sender Error: {e}")
            pass

    read_task = asyncio.create_task(reader(websocket))
    send_task = asyncio.create_task(sender(websocket))

    try:
        await asyncio.wait(
            [read_task, send_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        read_task.cancel()
        send_task.cancel()
        await pubsub.unsubscribe("metrics-update")
