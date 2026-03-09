FROM python:3.10-slim

WORKDIR /app

# Install Java and procps for PySpark
RUN apt-get update && \
    apt-get install -y default-jre-headless procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . /app

# Quick testing entrypoint
CMD ["python", "test/test_ingest.py"]
