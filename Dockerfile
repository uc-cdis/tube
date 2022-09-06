# To check running container: docker exec -it tube /bin/bash
FROM quay.io/cdis/python:python3.9-buster-stable

ENV DEBIAN_FRONTEND=noninteractive \
    SQOOP_VERSION="1.4.7" \
    HADOOP_VERSION="3.3.2" \
    ES_HADOOP_VERSION="8.3.3" \
    MAVEN_ES_URL="https://search.maven.org/remotecontent?filepath=org/elasticsearch" \
    ES_SPARK_30_2_12="elasticsearch-spark-30_2.12" \
    ES_SPARK_20_2_11="elasticsearch-spark-20_2.11"

ENV MAVEN_ES_SPARK_VERSION="${MAVEN_ES_URL}/${ES_SPARK_30_2_12}/${ES_HADOOP_VERSION}/${ES_SPARK_30_2_12}-${ES_HADOOP_VERSION}"

ENV SQOOP_INSTALLATION_URL="http://archive.apache.org/dist/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz" \
    HADOOP_INSTALLATION_URL="http://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    ES_HADOOP_INSTALLATION_URL="https://artifacts.elastic.co/downloads/elasticsearch-hadoop/elasticsearch-hadoop-${ES_HADOOP_VERSION}.zip" \
    SQOOP_HOME="/sqoop" \
    HADOOP_HOME="/hadoop" \
    ES_HADOOP_HOME="/es-hadoop" \
    JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64/"
ENV ES_HADOOP_HOME_VERSION="${ES_HADOOP_HOME}/elasticsearch-hadoop-${ES_HADOOP_VERSION}"

RUN mkdir -p /usr/share/man/man1
RUN mkdir -p /usr/share/man/man7

RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common \
    build-essential \
    openjdk-11-jdk-headless \
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
    libssl1.1 \
    libgnutls30 \
    vim \
    curl \
    g++ \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get --only-upgrade install libpq-dev

RUN pip install --upgrade poetry

RUN wget ${SQOOP_INSTALLATION_URL} \
    && mkdir -p $SQOOP_HOME \
    && tar -xvf sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz -C ${SQOOP_HOME} --strip-components 1 \
    && rm sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz \
    && rm -rf $SQOOP_HOME/docs

RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.4.jar -O $SQOOP_HOME/lib/postgresql-42.2.4.jar
RUN wget https://dlcdn.apache.org//commons/lang/binaries/commons-lang-2.6-bin.tar.gz \
    && tar -xvf commons-lang-2.6-bin.tar.gz \
    && rm commons-lang-2.6-bin.tar.gz \
    && mv commons-lang-2.6/commons-lang-2.6.jar $SQOOP_HOME/lib/

RUN wget ${HADOOP_INSTALLATION_URL} \
    && mkdir -p $HADOOP_HOME \
    && tar -xvf hadoop-${HADOOP_VERSION}.tar.gz -C ${HADOOP_HOME} --strip-components 1 \
    && rm hadoop-${HADOOP_VERSION}.tar.gz \
    && rm -rf $HADOOP_HOME/share/doc

RUN wget ${ES_HADOOP_INSTALLATION_URL} \
    && mkdir -p $ES_HADOOP_HOME \
    && unzip elasticsearch-hadoop-${ES_HADOOP_VERSION}.zip -d ${ES_HADOOP_HOME} \
    && rm elasticsearch-hadoop-${ES_HADOOP_VERSION}.zip

RUN wget ${MAVEN_ES_SPARK_VERSION}.jar -O ${ES_HADOOP_HOME_VERSION}/dist/${ES_SPARK_20_2_11}-${ES_HADOOP_VERSION}.jar
RUN wget ${MAVEN_ES_SPARK_VERSION}-javadoc.jar -O ${ES_HADOOP_HOME_VERSION}/dist/${ES_SPARK_20_2_11}-${ES_HADOOP_VERSION}-javadoc.jar
RUN wget ${MAVEN_ES_SPARK_VERSION}-sources.jar -O ${ES_HADOOP_HOME_VERSION}/dist/${ES_SPARK_20_2_11}-${ES_HADOOP_VERSION}-sources.jar

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

COPY . /tube
WORKDIR /tube

RUN  python3.9 -m pip install -r requirements.txt

#ENV TINI_VERSION v0.18.0
#ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
#RUN chmod +x /tini
#ENTRYPOINT ["/tini", "--"]

ENV PYTHONUNBUFFERED 1
