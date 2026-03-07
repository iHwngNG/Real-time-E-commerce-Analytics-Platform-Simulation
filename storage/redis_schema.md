# Redis Data Model — F4.5
=============================================================================
> Purpose: Fast lookup layer for realtime counters, leaderboards, and pub/sub notifications.
> Size Management: Keys use reasonable TTLs (e.g., active sessions) to prevent memory leaks in production, while unbounded keys (like leaderboards) should theoretically be pruned by a cleanup cron job if dataset grows infinitely.

## Key Patterns

| Key pattern | Type | TTL | Content |
|---|---|---|---|
| `metric:summary` | HASH | None (Overwrite) | Fields representing real-time Dashboard KPIs:<br>- `click_1m`: Total clicks last min<br>- `cart_1m`: Add to carts<br>- `purchase_1m`: Purchases<br>- `revenue_1m`: Revenue last min |
| `top:products:click` | ZSET (Sorted Set) | None (Unbounded) | Top 10 viewed/clicked products. Score = click_count. |
| `top:products:purchase`| ZSET (Sorted Set) | None (Unbounded) | Top 10 purchased products. Score = purchase_count. |
| `session:{session_id}` | STRING | 1800s (30m) | Exists as long as a user acts on the platform within 30 minutes. Used to detect Active Unique Users. |

## Pub/Sub Channels

| Channel name | Triggered By | Subscribed By | Purpose |
|---|---|---|---|
| `metrics-update` | PySpark Sink (`redis_sink.py`) | FastAPI (`websocket.py`) | Triggered every 30s when Spark finalizes a micro-batch. FastAPI forwards this ping to React Dashboard to fetch via REST or push directly. |

## Interaction Flow
- `Redis Sink (F3.4.2)` writes to `HASH` and `ZSET`. Then executes a `PUBLISH` command on channel.
- `API Layer (F7)` uses an async Redis client. Once it receives a message from `SUBSCRIBE`, it sends `{ event: "refresh" }` to connected WebSockets.
