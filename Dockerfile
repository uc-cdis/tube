ARG AZLINUX_BASE_VERSION=3.13-pythonnginx

FROM quay.io/cdis/amazonlinux-base:${AZLINUX_BASE_VERSION} AS base

ENV appname=tube

WORKDIR /${appname}

RUN chown -R gen3:gen3 /${appname}

# Builder stage
FROM base AS builder

USER root
# Adding this specifically to get Tube dependencies to build properly for ARM images only.
RUN dnf -y update && \
    dnf -y groupinstall "Development Tools" && \
    dnf -y install \
      python3-devel \
      postgresql-devel \
    && dnf clean all

USER gen3

COPY poetry.lock pyproject.toml README.md /${appname}/

RUN poetry install -vv --only main --no-interaction

COPY --chown=gen3:gen3 . /${appname}

# Install the app
RUN poetry install --without dev --no-interaction

FROM base

USER root

COPY --from=builder /${appname} /${appname}

ENV SQOOP_VERSION="1.4.7" \
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
    JAVA_HOME="/usr"
ENV ES_HADOOP_HOME_VERSION="${ES_HADOOP_HOME}/elasticsearch-hadoop-${ES_HADOOP_VERSION}" \
    HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop \
    HADOOP_MAPRED_HOME=$HADOOP_HOME \
    HADOOP_COMMON_HOME=$HADOOP_HOME \
    HADOOP_HDFS_HOME=$HADOOP_HOME \
    HADOOP_USER_NAME=gen3 \
    YARN_HOME=$HADOOP_HOME \
    ACCUMULO_HOME=/accumulo \
    HIVE_HOME=/hive \
    HBASE_HOME=/hbase \
    HCAT_HOME=/hcatalog \
    ZOOKEEPER_HOME=/zookeeper \
    HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native \
    LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH

RUN dnf -y update \
    && dnf -y install \
    java-17-amazon-corretto \
    postgresql15 \
    tar \
    unzip \
    vim \
    wget && \
    dnf clean all

RUN wget --quiet --no-verbose ${SQOOP_INSTALLATION_URL} \
    && mkdir -p $SQOOP_HOME \
    && tar -xvf sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz -C ${SQOOP_HOME} --strip-components 1 \
    && rm sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz \
    && rm -rf $SQOOP_HOME/docs

RUN wget --quiet --no-verbose https://jdbc.postgresql.org/download/postgresql-42.2.4.jar -O $SQOOP_HOME/lib/postgresql-42.2.4.jar \
    && wget --quiet --no-verbose https://dlcdn.apache.org//commons/lang/binaries/commons-lang-2.6-bin.tar.gz \
    && tar -xvf commons-lang-2.6-bin.tar.gz \
    && rm commons-lang-2.6-bin.tar.gz \
    && mv commons-lang-2.6/commons-lang-2.6.jar $SQOOP_HOME/lib/

RUN wget --quiet --no-verbose ${HADOOP_INSTALLATION_URL} \
    && mkdir -p $HADOOP_HOME \
    && tar -xvf hadoop-${HADOOP_VERSION}.tar.gz -C ${HADOOP_HOME} --strip-components 1 \
    && rm hadoop-${HADOOP_VERSION}.tar.gz \
    && rm -rf $HADOOP_HOME/share/doc

RUN wget --quiet --no-verbose ${ES_HADOOP_INSTALLATION_URL} \
    && mkdir -p $ES_HADOOP_HOME \
    && unzip elasticsearch-hadoop-${ES_HADOOP_VERSION}.zip -d ${ES_HADOOP_HOME} \
    && rm elasticsearch-hadoop-${ES_HADOOP_VERSION}.zip

RUN wget --quiet --no-verbose ${MAVEN_ES_SPARK_VERSION}.jar -O ${ES_HADOOP_HOME_VERSION}/dist/${ES_SPARK_20_2_11}-${ES_HADOOP_VERSION}.jar \
    && wget --quiet --no-verbose ${MAVEN_ES_SPARK_VERSION}-javadoc.jar -O ${ES_HADOOP_HOME_VERSION}/dist/${ES_SPARK_20_2_11}-${ES_HADOOP_VERSION}-javadoc.jar \
    && wget --quiet --no-verbose ${MAVEN_ES_SPARK_VERSION}-sources.jar -O ${ES_HADOOP_HOME_VERSION}/dist/${ES_SPARK_20_2_11}-${ES_HADOOP_VERSION}-sources.jar

RUN mkdir -p $ACCUMULO_HOME $HIVE_HOME $HBASE_HOME $HCAT_HOME $ZOOKEEPER_HOME && \
    chown -R gen3:gen3 $HADOOP_HOME && \
    mkdir -p /result && \
    chown -R gen3:gen3 /result

ENV PATH=${SQOOP_HOME}/bin:${HADOOP_HOME}/sbin:$HADOOP_HOME/bin:${JAVA_HOME}/bin:${PATH}

USER gen3

ENV PATH="/venv/bin:$PATH" \
    VIRTUAL_ENV="/venv" \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8


WORKDIR /${appname}

# install the app
RUN poetry install --without dev --no-interaction
