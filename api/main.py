from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import metrics, reports, websocket
from services.postgres_service import postgres_db
from services.redis_service import redis_db

app = FastAPI(title="Real-time E-commerce Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    await postgres_db.connect()
    await redis_db.connect()


@app.on_event("shutdown")
async def shutdown_event():
    await postgres_db.disconnect()
    await redis_db.disconnect()


app.include_router(metrics.router, prefix="/api/v1/metrics", tags=["Metrics"])
app.include_router(reports.router, prefix="/api/v1/reports", tags=["Reports"])
app.include_router(websocket.router, tags=["WebSocket"])


@app.get("/api/v1/health")
async def health_check():
    return {"status": "ok", "services": {"postgres": "connected", "redis": "connected"}}


@app.post("/internal/cache/invalidate")
async def invalidate_cache():
    return {"status": "success", "message": "Cache invalidated"}
