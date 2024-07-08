FROM quay.io/cdis/spark-base:3.3.0-hadoop3.3

LABEL maintainer="Gezim Sejdiu <g.sejdiu@gmail.com>, Giannis Mouchakis <gmouchakis@gmail.com>"

ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_WORKER_LOG /spark/logs
ENV SPARK_MASTER "spark://spark-master:7077"

WORKDIR /tube

# copy ONLY poetry artifact, install the dependencies but not fence
# this will make sure than the dependencies is cached
COPY poetry.lock pyproject.toml /tube/
RUN python -m poetry config virtualenvs.create false \
    && python -m poetry install -vv --no-root --only main --no-interaction \
    && python -m poetry show -v

# copy source code ONLY after installing dependencies
COPY tube /tube
COPY README.md setup.py /tube/

RUN python -m poetry config virtualenvs.create false \
    && python -m poetry install -vv --only main --no-interaction \
    && python -m poetry show -v

RUN python setup.py sdist bdist_wheel

EXPOSE 8081

COPY dockers/spark/worker/worker.sh /

CMD ["/bin/bash", "/worker.sh"]