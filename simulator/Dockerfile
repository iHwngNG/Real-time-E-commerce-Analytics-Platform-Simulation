# =============================================================================
# Simulator Dockerfile — F1
# Purpose: Container for data seeding and real-time event simulation.
# =============================================================================
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python", "main.py"]
CMD ["--mode", "simulate"]
