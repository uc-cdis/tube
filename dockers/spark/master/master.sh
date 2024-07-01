#!/bin/bash

export SPARK_MASTER_HOST=${SPARK_MASTER_HOST:-`hostname`}

export SPARK_HOME=/spark

. "/spark/sbin/spark-config.sh"

. "/spark/bin/load-spark-env.sh"

mkdir -p $SPARK_MASTER_LOG

ln -sf /dev/stdout $SPARK_MASTER_LOG/spark-master.out

cp /spark/conf/log4j2.properties.template /spark/conf/log4j2.properties
sed -i 's/= info/= DEBUG/g' /spark/conf/log4j2.properties
echo "logger.org.apache.spark=DEBUG" >> /spark/conf/log4j2.properties

function addConfig() {
  local path=$1
  local name=$2
  local value=$3

  local entry="<property><name>$name</name><value>${value}</value></property>"
  local escapedEntry=$(echo $entry | sed 's/\//\\\//g')
  sed -i "/<\/configuration>/ s/.*/${escapedEntry}\n&/" $path
}

cd /spark/bin && /spark/sbin/../bin/spark-class -Dlog4j.configuration=file:/spark/conf/log4j2.properties org.apache.spark.deploy.master.Master \
    --ip $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT >> $SPARK_MASTER_LOG/spark-master.out