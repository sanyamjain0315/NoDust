FROM apache/spark:4.1.1
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/spark-apps
COPY requirements.txt /opt/spark-apps
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py /opt/spark-apps/main.py
COPY system/ /opt/spark-apps/system/
ENV IVY_CACHE_DIR=/tmp \
    IVY_HOME=/tmp
USER spark
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
    "--num-executors", "1", \
    "--executor-cores", "1", \
    "--executor-memory", "1G", \
    "--driver-memory", "512m", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,software.amazon.msk:aws-msk-iam-auth:2.2.0", \
    "--conf", "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp", \
    "--conf", "spark.executor.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp", \
    "/opt/spark-apps/main.py", "--mode", "anomaly"]
