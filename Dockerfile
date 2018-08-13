# To check running container: docker exec -it tube /bin/bash
FROM python:2

ENV DEBIAN_FRONTEND=noninteractive \
    SQOOP_VERSION="1.4.7" \
    HADOOP_VERSION="3.1.0" \
    ES_HADOOP_VERSION="6.3.2"

ENV SQOOP_INSTALLATION_URL="http://archive.apache.org/dist/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz" \
    HADOOP_INSTALLATION_URL="http://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    ES_HADOOP_INSTALLATION_URL="https://artifacts.elastic.co/downloads/elasticsearch-hadoop/elasticsearch-hadoop-${ES_HADOOP_VERSION}.zip"

ENV SQOOP_HOME="/sqoop" \
    HADOOP_HOME="/hadoop" \
    ES_HADOOP_HOME="/es-hadoop" \
    JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/"

RUN apt-get update && apt-get install -y --no-install-recommends \
    apache2 \
    build-essential \
    curl \
    wget \
    git \
    openjdk-8-jdk \
    postgresql-client \
    unzip \
    # dependency for cryptography
    libffi-dev \
    # dependency for pyscopg2 - which is dependency for sqlalchemy postgres engine
    libpq-dev \
    # dependency for cryptography
    libssl-dev \
    python-dev \
    python-pip \
    python-setuptools \
    vim \
    net-tools

RUN pip install pip==9.0.3
RUN pip install --upgrade pip
RUN pip install --upgrade setuptools

RUN wget ${SQOOP_INSTALLATION_URL}
RUN mkdir -p $SQOOP_HOME
RUN tar -xvf sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz -C ${SQOOP_HOME} --strip-components 1
RUN rm sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz
RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.4.jar
RUN mv postgresql-42.2.4.jar $SQOOP_HOME/lib/

RUN wget ${HADOOP_INSTALLATION_URL}
RUN mkdir -p $HADOOP_HOME
RUN tar -xvf hadoop-${HADOOP_VERSION}.tar.gz -C ${HADOOP_HOME} --strip-components 1
RUN rm hadoop-${HADOOP_VERSION}.tar.gz

RUN wget ${ES_HADOOP_INSTALLATION_URL}
RUN mkdir -p $ES_HADOOP_HOME
RUN unzip elasticsearch-hadoop-${ES_HADOOP_VERSION}.zip -d ${ES_HADOOP_HOME}
RUN rm elasticsearch-hadoop-${ES_HADOOP_VERSION}.zip

ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop \
    HADOOP_MAPRED_HOME=$HADOOP_HOME \
    HADOOP_COMMON_HOME=$HADOOP_HOME \
    HADOOP_HDFS_HOME=$HADOOP_HOME \
    YARN_HOME=$HADOOP_HOME \
    HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native

ENV PATH="${PATH}:${SQOOP_HOME}/bin:${HADOOP_HOME}/sbin:$HADOOP_HOME/bin:${JAVA_HOME}/bin"

COPY . /tube
WORKDIR /tube

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

RUN pip install --no-cache-dir -r requirements.txt
RUN python setup.py develop
