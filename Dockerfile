FROM python:3.10-slim

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=3.1.5
ENV PYTHON_VERSION=3.10

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR ${AIRFLOW_HOME}

# Install Apache Airflow with PostgreSQL support
RUN pip install --no-cache-dir uv && \
    uv pip install --system "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Copy requirements if exists
# COPY requirements.txt* ./
# RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

# Copy DAGs, plugins, and config
COPY dags ${AIRFLOW_HOME}/dags
COPY plugins ${AIRFLOW_HOME}/plugins
# COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

# Create necessary directories
# RUN mkdir -p ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins

# Expose port for Airflow webserver
EXPOSE 8080

# Run Airflow in standalone mode
CMD ["airflow", "standalone"]
