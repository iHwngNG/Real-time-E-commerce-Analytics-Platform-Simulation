from fastapi import APIRouter
from datetime import datetime
from services.postgres_service import postgres_db

router = APIRouter()


@router.get("/daily")
async def get_daily_report():
    query = "SELECT * FROM mart_daily_summary ORDER BY date DESC LIMIT 30"
    results = await postgres_db.fetch(query)
    return {
        "data": results,
        "meta": {"count": len(results), "generated_at": datetime.utcnow().isoformat()},
    }


@router.get("/funnel")
async def get_funnel_report():
    query = "SELECT * FROM mart_funnel_analysis ORDER BY date DESC LIMIT 30"
    results = await postgres_db.fetch(query)
    return {
        "data": results,
        "meta": {"count": len(results), "generated_at": datetime.utcnow().isoformat()},
    }


@router.get("/products")
async def get_products_report():
    query = "SELECT * FROM mart_product_performance ORDER BY date DESC LIMIT 100"
    results = await postgres_db.fetch(query)
    return {
        "data": results,
        "meta": {"count": len(results), "generated_at": datetime.utcnow().isoformat()},
    }
