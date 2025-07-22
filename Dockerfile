ARG AZLINUX_BASE_VERSION=master
FROM quay.io/cdis/python-build-base:${AZLINUX_BASE_VERSION} AS base

# Define all environment variables early
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

# Install system packages (rarely change)
RUN dnf -y update \
    && dnf -y install \
    java-11-amazon-corretto \
    postgresql15 \
    tar \
    unzip \
    vim \
    wget && \
    dnf clean all

# Create user and group
RUN groupadd -g 1000 gen3 && \
    useradd -m -s /bin/bash -u 1000 -g gen3 gen3

# Create all necessary directories
RUN mkdir -p $SQOOP_HOME $HADOOP_HOME $ES_HADOOP_HOME \
    $ACCUMULO_HOME $HIVE_HOME $HBASE_HOME $HCAT_HOME $ZOOKEEPER_HOME /result

# Download and install Sqoop (slow, rarely changes)
RUN wget --quiet --no-verbose ${SQOOP_INSTALLATION_URL} \
    && tar -xvf sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz -C ${SQOOP_HOME} --strip-components 1 \
    && rm sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz \
    && rm -rf $SQOOP_HOME/docs

# Download PostgreSQL driver and commons-lang (slow, rarely changes)
RUN wget --quiet --no-verbose https://jdbc.postgresql.org/download/postgresql-42.2.4.jar -O $SQOOP_HOME/lib/postgresql-42.2.4.jar \
    && wget --quiet --no-verbose https://dlcdn.apache.org//commons/lang/binaries/commons-lang-2.6-bin.tar.gz \
    && tar -xvf commons-lang-2.6-bin.tar.gz \
    && rm commons-lang-2.6-bin.tar.gz \
    && mv commons-lang-2.6/commons-lang-2.6.jar $SQOOP_HOME/lib/

# Download and install Hadoop (slow, rarely changes)
RUN wget --quiet --no-verbose ${HADOOP_INSTALLATION_URL} \
    && tar -xvf hadoop-${HADOOP_VERSION}.tar.gz -C ${HADOOP_HOME} --strip-components 1 \
    && rm hadoop-${HADOOP_VERSION}.tar.gz \
    && rm -rf $HADOOP_HOME/share/doc

# Download and install Elasticsearch Hadoop (slow, rarely changes)
RUN wget --quiet --no-verbose ${ES_HADOOP_INSTALLATION_URL} \
    && unzip elasticsearch-hadoop-${ES_HADOOP_VERSION}.zip -d ${ES_HADOOP_HOME} \
    && rm elasticsearch-hadoop-${ES_HADOOP_VERSION}.zip

# Download Elasticsearch Spark connectors (slow, rarely changes)
RUN wget --quiet --no-verbose ${MAVEN_ES_SPARK_VERSION}.jar -O ${ES_HADOOP_HOME_VERSION}/dist/${ES_SPARK_20_2_11}-${ES_HADOOP_VERSION}.jar \
    && wget --quiet --no-verbose ${MAVEN_ES_SPARK_VERSION}-javadoc.jar -O ${ES_HADOOP_HOME_VERSION}/dist/${ES_SPARK_20_2_11}-${ES_HADOOP_VERSION}-javadoc.jar \
    && wget --quiet --no-verbose ${MAVEN_ES_SPARK_VERSION}-sources.jar -O ${ES_HADOOP_HOME_VERSION}/dist/${ES_SPARK_20_2_11}-${ES_HADOOP_VERSION}-sources.jar

# Set ownership for infrastructure components
RUN chown -R gen3:gen3 $HADOOP_HOME /result

# Set PATH for infrastructure tools
ENV PATH=${SQOOP_HOME}/bin:${HADOOP_HOME}/sbin:$HADOOP_HOME/bin:${JAVA_HOME}/bin:${PATH}

# Final stage - Python dependencies and source code (changes more frequently)
FROM base AS final

ENV appname=tube
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1

WORKDIR /${appname}

# Set ownership for app directory
RUN chown -R gen3:gen3 /${appname} && \
    chown -R gen3:gen3 /venv

USER gen3

# Create virtual environment
RUN python -m venv /venv

# Install poetry
RUN pip install --no-cache-dir 'poetry<2.0'

# Copy dependency files first (cached until dependencies change)
COPY poetry.lock pyproject.toml README.md /${appname}/

# Install Python dependencies (cached until poetry files change)
RUN poetry install -vv --only main --no-interaction

# Copy source code last (changes most frequently)
COPY --chown=gen3:gen3 . /${appname}

# Final poetry install for source-dependent setup
RUN poetry install --without dev --no-interaction

# Set Python environment variables
ENV PATH="/venv/bin:$PATH" \
    VIRTUAL_ENV="/venv" \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8