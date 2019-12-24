# To check running container: docker exec -it tube /bin/bash
FROM python:3.7-stretch

ENV DEBIAN_FRONTEND=noninteractive \
    SQOOP_VERSION="1.4.7" \
    HADOOP_VERSION="3.1.1" \
    ES_HADOOP_VERSION="6.4.0"

ENV SQOOP_INSTALLATION_URL="http://archive.apache.org/dist/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz" \
    HADOOP_INSTALLATION_URL="http://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    ES_HADOOP_INSTALLATION_URL="https://artifacts.elastic.co/downloads/elasticsearch-hadoop/elasticsearch-hadoop-${ES_HADOOP_VERSION}.zip"\
    SQOOP_HOME="/sqoop" \
    HADOOP_HOME="/hadoop" \
    ES_HADOOP_HOME="/es-hadoop" \
    JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/"

RUN mkdir -p /usr/share/man/man1
RUN mkdir -p /usr/share/man/man7

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    openjdk-8-jdk-headless \
    # dependency for pyscopg2 - which is dependency for sqlalchemy postgres engine
    libpq-dev \
    postgresql-client \
    wget \
    unzip \
    git \
    # dependency for cryptography
    libffi-dev \
    # dependency for cryptography
    libssl-dev \
    vim \
    curl \
    && rm -rf /var/lib/apt/lists/*

#RUN pip install pip==9.0.3
#RUN pip install --upgrade pip
#RUN pip install --upgrade setuptools

RUN wget ${SQOOP_INSTALLATION_URL} \
    && mkdir -p $SQOOP_HOME \
    && tar -xvf sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz -C ${SQOOP_HOME} --strip-components 1 \
    && rm sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz \
    && rm -rf $SQOOP_HOME/docs

RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.4.jar -O $SQOOP_HOME/lib/postgresql-42.2.4.jar

RUN wget ${HADOOP_INSTALLATION_URL} \
    && mkdir -p $HADOOP_HOME \
    && tar -xvf hadoop-${HADOOP_VERSION}.tar.gz -C ${HADOOP_HOME} --strip-components 1 \
    && rm hadoop-${HADOOP_VERSION}.tar.gz \
    && rm -rf $HADOOP_HOME/share/doc

RUN wget ${ES_HADOOP_INSTALLATION_URL} \
    && mkdir -p $ES_HADOOP_HOME \
    && unzip elasticsearch-hadoop-${ES_HADOOP_VERSION}.zip -d ${ES_HADOOP_HOME} \
    && rm elasticsearch-hadoop-${ES_HADOOP_VERSION}.zip

ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop \
    HADOOP_MAPRED_HOME=$HADOOP_HOME \
    HADOOP_COMMON_HOME=$HADOOP_HOME \
    HADOOP_HDFS_HOME=$HADOOP_HOME \
    YARN_HOME=$HADOOP_HOME \
    ACCUMULO_HOME=/accumulo \
    HIVE_HOME=/hive \
    HBASE_HOME=/hbase \
    HCAT_HOME=/hcatalog \
    ZOOKEEPER_HOME=/zookeeper \
    HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native \
    LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH

RUN mkdir -p $ACCUMULO_HOME $HIVE_HOME $HBASE_HOME $HCAT_HOME $ZOOKEEPER_HOME

ENV PATH=${SQOOP_HOME}/bin:${HADOOP_HOME}/sbin:$HADOOP_HOME/bin:${JAVA_HOME}/bin:${PATH}

COPY requirements.txt /tube/
RUN pip install --no-cache-dir -r /tube/requirements.txt

COPY . /tube
WORKDIR /tube

#ENV TINI_VERSION v0.18.0
#ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
#RUN chmod +x /tini
#ENTRYPOINT ["/tini", "--"]

RUN python setup.py develop
