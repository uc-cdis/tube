ARG AZLINUX_BASE_VERSION=master

# Base stage with python-build-base
FROM quay.io/cdis/python-build-base:${AZLINUX_BASE_VERSION} AS base


ENV appname=tube
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1

WORKDIR /${appname}

# create gen3 user
# Create a group 'gen3' with GID 1000 and a user 'gen3' with UID 1000
RUN groupadd -g 1000 gen3 && \
    useradd -m -s /bin/bash -u 1000 -g gen3 gen3  && \
    chown -R gen3:gen3 /${appname} && \
    chown -R gen3:gen3 /venv

# Builder stage
FROM base AS builder

USER gen3

RUN python -m venv /venv

COPY poetry.lock pyproject.toml README.md /${appname}/

RUN pip install poetry && \
    poetry install -vv --only main --no-interaction

COPY --chown=gen3:gen3 . /${appname}

RUN git config --global --add safe.directory /${appname} && COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" > /$appname/version_data.py \
    && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >> /$appname/version_data.py

# Run poetry again so this app itself gets installed too
RUN poetry install --without dev --no-interaction

# Final stage
FROM base

COPY --from=builder /venv /venv
COPY --from=builder /${appname} /${appname}


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
    JAVA_HOME="/usr"
ENV ES_HADOOP_HOME_VERSION="${ES_HADOOP_HOME}/elasticsearch-hadoop-${ES_HADOOP_VERSION}"

RUN mkdir -p /usr/share/man/man1
RUN mkdir -p /usr/share/man/man7


RUN dnf -y update
RUN dnf -y install \
    wget tar unzip vim
RUN dnf -y install java-11-amazon-corretto


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
RUN chown -R gen3:gen3 $HADOOP_HOME

ENV PATH=${SQOOP_HOME}/bin:${HADOOP_HOME}/sbin:$HADOOP_HOME/bin:${JAVA_HOME}/bin:${PATH}


# Switch to non-root user 'gen3' for the serving process
USER gen3

RUN source /venv/bin/activate

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8
