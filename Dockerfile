# To check running container: docker exec -it tube /bin/bash
FROM quay.io/cdis/spark-base:3.3.0-hadoop3.3

ENV SQOOP_VERSION="1.4.7"

ENV SQOOP_INSTALLATION_URL="http://archive.apache.org/dist/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz"

RUN mkdir -p /usr/share/man/man1
RUN mkdir -p /usr/share/man/man7

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip poetry requests

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

ENV PATH=${SQOOP_HOME}/bin:${HADOOP_HOME}/sbin:$HADOOP_HOME/bin:${JAVA_HOME}/bin:${PATH}

WORKDIR /tube

# copy ONLY poetry artifact, install the dependencies but not fence
# this will make sure than the dependencies is cached
COPY poetry.lock pyproject.toml /tube/
RUN python -m poetry config virtualenvs.create false \
    && python -m poetry install -vv --no-root --only main --no-interaction \
    && python -m poetry show -v

# copy source code ONLY after installing dependencies
COPY . /tube
COPY dockers/confs/log4j.properties /spark/conf/log4j.properties
COPY dockers/confs/log4j2.properties /spark/conf/log4j2.properties

RUN python -m poetry config virtualenvs.create false \
    && python -m poetry install -vv --only main --no-interaction \
    && python -m poetry show -v

EXPOSE 22 4040 7077 8020 8030 8031 8032 8042 8088 9000

ENV PYTHONUNBUFFERED 1
