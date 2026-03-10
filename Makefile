# =============================================================================
# Project Makefile — Cung cấp các "Nút bấm" để vận hành hệ thống dễ dàng
# Usage: 
#   make up        - Bật toàn bộ hạ tầng (DB, Kafka, Redis, Airflow)
#   make down      - Tắt toàn bộ hệ thống
#   make seed      - Nạp dữ liệu Users & Products mẫu
#   make simulate  - Bắt đầu phát dữ liệu Event thời gian thực
#   make both      - Nạp dữ liệu xong phát ngay lập tức
#   make stream    - Chạy động cơ Spark Streaming (sau khi build main.py)
# =============================================================================

.PHONY: up down seed simulate both stream logs status

# --- Infra Control ---
up:
	docker compose up -d

down:
	docker compose down -v

status:
	docker compose ps

logs:
	docker compose logs -f

# --- Simulator "Buttons" ---
seed:
	docker compose run --rm simulator --mode seed

simulate:
	docker compose run --rm simulator --mode simulate

both:
	docker compose run --rm simulator --mode both

# --- Streaming Engine ---
stream:
	docker compose run --rm streaming python main.py

# --- Helpers ---
clean:
	rm -rf dbt/target dbt/logs dbt/dbt_packages
	docker system prune -f
