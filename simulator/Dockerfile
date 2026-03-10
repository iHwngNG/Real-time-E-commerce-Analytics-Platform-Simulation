# =============================================================================
# Simulator Dockerfile — F1
# Purpose: Container for data seeding and real-time event simulation.
# =============================================================================
FROM python:3.12-slim

# Optimize Python execution
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Cài đặt dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY . .

# Chạy với user không phải là root (best practice bảo mật)
RUN adduser --uid 1000 --disabled-password --gecos "" appuser \
    && chown -R appuser /app
USER appuser

ENTRYPOINT ["python", "main.py"]
CMD ["--mode", "simulate"]
