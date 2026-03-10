# Error & Debugging Logs

| Error Name | Object | Error Type | Debug Method & Solution |
|---|---|---|---|
| KeyError (Kafka Sink) | `kafka_sink.py` | Python Logic | Added `.alias()` to casted fields in the Spark `struct` to preserve field names during JSON serialization. |
| java.net.UnknownHostException | `test_postgres_sink.py` | Network | Configured `DATABASE_URL` with correct Docker service name (`postgres`) and port (`5432`) to ensure network reachability within the container network. |
| JDBC Connection Error | `postgres_sink.py` | Configuration | Parsed DATABASE_URL into separate properties (user, password) as PySpark JDBC does not support the `user:pass@host` URL format. |
| UUID Casting Error | `postgres_sink.py` | DB Schema | Added `"stringtype": "unspecified"` to JDBC properties to allow PostgreSQL driver to automatically cast Spark Strings to database UUIDs. |
| ModuleNotFoundError: 'redis' | `test_redis_sink.py` | Dependency | Re-built the Docker image after adding `redis` package to `streaming/requirements.txt`. |
| public_public Schema Error | `dbt_project.yml` | dbt Config | Removed explicit `+schema: public` from model configurations as dbt-postgres already handles the schema mapping correctly, preventing double prefixing. |
| Relation Not Found | `dbt test` | dbt State | Ensured models were built (`dbt run`) before running tests (`dbt test`) to ensure relations exist in the database. |
| Spark Analyzer Error (SameContext) | Streaming Pipeline (F3) | PySpark Analysis | Chuyển sang Independent Sinks thay vì Union. Chỉ áp dụng `withWatermark` một lần tại source. |
| Unique Violation (Postgres) | Postgres Sink | DB Constraint | Dùng Staging Table + `ON CONFLICT` SQL thực thi qua Spark JVM bridge. |
| Permission Denied (mkdir) | Spark Checkpoints | OS / Permissions | Chuyển checkpoint path vào `/app/checkpoints` và `chown` cho appuser trong Dockerfile. |
| Spark Driver Memory (OOM) | Streaming Module | Resource | Tăng `SPARK_DRIVER_MEMORY` lên 1G và thiết lập `limits` trong docker-compose. |
