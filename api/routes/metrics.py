from fastapi import APIRouter, Query
from typing import Optional
from datetime import datetime
from services.redis_service import redis_db
from services.postgres_service import postgres_db
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/summary")
async def get_summary():
    # Redis HASH | KPIs hiện tại
    redis = redis_db.get_client()
    data = await redis.hgetall("metric:summary")
    return {"data": data, "meta": {"generated_at": datetime.utcnow().isoformat()}}


@router.get("/timeseries")
async def get_timeseries(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    window_type: str = "1m",
    limit: int = 100,
):
    # PostgreSQL aggregated_metrics
    query = """
        SELECT window_start, metric_name, metric_value, dimension_key, dimension_value
        FROM aggregated_metrics
        WHERE window_type = $1
    """
    args = [window_type]

    if from_date and to_date:
        query += " AND window_start >= $2 AND window_start <= $3"
        args.extend(
            [datetime.fromisoformat(from_date), datetime.fromisoformat(to_date)]
        )

    query += f" ORDER BY window_start DESC LIMIT ${len(args) + 1}"
    args.append(limit)

    results = await postgres_db.fetch(query, *args)
    return {
        "data": results,
        "meta": {"count": len(results), "generated_at": datetime.utcnow().isoformat()},
    }


@router.get("/top-products")
async def get_top_products(metric: str = "click"):
    # Redis ZSET
    redis = redis_db.get_client()
    key = f"top:products:{metric}"
    try:
        results = await redis.zrange(key, 0, 9, desc=True, withscores=True)
    except Exception as e:
        logger.error(f"Error getting top products: {e}")
        results = []

    data = [{"product_id": item[0], "score": item[1]} for item in results]
    return {"data": data, "meta": {"generated_at": datetime.utcnow().isoformat()}}


@router.get("/breakdown")
async def get_breakdown(
    dimension: str = Query(..., description="Dimension to breakdown by")
):
    query = """
        SELECT window_start, metric_name, metric_value, dimension_value
        FROM aggregated_metrics
        WHERE dimension_key = $1
        ORDER BY window_start DESC LIMIT 100
    """
    results = await postgres_db.fetch(query, dimension)
    return {
        "data": results,
        "meta": {"count": len(results), "generated_at": datetime.utcnow().isoformat()},
    }
