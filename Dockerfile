# Use the official Airflow slim image as base
FROM apache/airflow:slim-2.6.0-python3.10

# Switch to root user for installations
USER root

# Install only essential build dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        nano \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy the crypto pipeline code
COPY --chown=airflow:0 . ./

# Upgrade pip and install dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

