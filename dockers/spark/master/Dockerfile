FROM quay.io/cdis/spark-base:3.3.0-hadoop3.3

LABEL maintainer="Gezim Sejdiu <g.sejdiu@gmail.com>, Giannis Mouchakis <gmouchakis@gmail.com>"

ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_LOG /spark/logs

RUN mkdir /tube
WORKDIR /tube

# copy ONLY poetry artifact, install the dependencies but not fence
# this will make sure than the dependencies is cached
COPY poetry.lock pyproject.toml /tube/
RUN python -m poetry config virtualenvs.create false \
    && python -m poetry install -vv --no-root --only main --no-interaction \
    && python -m poetry show -v

# copy source code ONLY after installing dependencies
COPY . /tube/
COPY README.md setup.py /tube/

RUN python -m poetry config virtualenvs.create false \
    && python -m poetry install -vv --only main --no-interaction \
    && python -m poetry build \
    && python -m poetry show -v

RUN python -m pip install --upgrade pip
RUN python -m pip install dist/tube-1.0.5-py3-none-any.whl

EXPOSE 8080 7077 6066

COPY dockers/spark/master/master.sh /
COPY dockers/confs/log4j.properties /spark/conf/log4j.properties
COPY dockers/confs/log4j2.properties /spark/conf/log4j2.properties

CMD ["/bin/bash", "/master.sh"]
