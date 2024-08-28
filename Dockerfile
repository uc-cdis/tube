FROM quay.io/cdis/spark-base:3.3.0-hadoop3.3

ENV SQOOP_HOME="/sqoop" \
    SQOOP_VERSION="1.4.7"

ENV SQOOP_INSTALLATION_URL="http://archive.apache.org/dist/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz"
ENV ES_HADOOP_HOME_VERSION="${ES_HADOOP_HOME}/elasticsearch-hadoop-${ES_HADOOP_VERSION}"
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    unzip \
    && rm -rf /var/lib/apt/lists/*

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

ENV PATH=${SQOOP_HOME}/bin:${PATH}

WORKDIR /tube

# copy ONLY poetry artifact, install the dependencies but not fence
# this will make sure than the dependencies is cached
COPY poetry.lock pyproject.toml /tube/
RUN python -m poetry config virtualenvs.create false \
    && python -m poetry install -vv --no-root --only main --no-interaction \
    && python -m poetry show -v

# copy source code ONLY after installing dependencies
COPY . /tube
#COPY dockers/confs/log4j.properties /spark/conf/log4j.properties
#COPY dockers/confs/log4j2.properties /spark/conf/log4j2.properties

RUN python -m poetry config virtualenvs.create false \
    && python -m poetry install -vv --only main --no-interaction \
    && python -m poetry show -v

#ENV TINI_VERSION v0.18.0
#ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
#RUN chmod +x /tini
#ENTRYPOINT ["/tini", "--"]

ENV PYTHONUNBUFFERED 1