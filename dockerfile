FROM apache/spark:4.1.1
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyyaml pandas pyarrow
USER spark
WORKDIR /opt/spark-apps
