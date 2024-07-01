#!/bin/bash

export SPARK_HOME=/spark

. "/spark/sbin/spark-config.sh"

. "/spark/bin/load-spark-env.sh"

mkdir -p $SPARK_WORKER_LOG

ln -sf /dev/stdout $SPARK_WORKER_LOG/spark-worker.out

cp /spark/conf/log4j2.properties.template /spark/conf/log4j2.properties
sed -i 's/= info/= DEBUG/g' /spark/conf/log4j2.properties
echo "logger.org.apache.spark=DEBUG" >> /spark/conf/log4j2.properties

/spark/sbin/../bin/spark-class -Dlog4j.configuration=file:/spark/conf/log4j2.properties org.apache.spark.deploy.worker.Worker \
    --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG/spark-worker.out