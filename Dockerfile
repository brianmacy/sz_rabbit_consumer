# docker build -t brian/sz_rabbit_consumer .
# docker run --user $UID -it -v $PWD:/data -e SENZING_ENGINE_CONFIGURATION_JSON brian/sz_rabbit_consumer

ARG BASE_IMAGE=senzing/senzingapi-runtime:latest
FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2022-08-27

LABEL Name="brain/sz_rabbit_consumer" \
      Maintainer="brianmacy@gmail.com" \
      Version="DEV"

RUN apt-get update \
 && apt-get -y install curl python3 python3-pip python3-pika unzip libaio1 \
 && python3 -mpip install orjson \
 && apt-get -y remove build-essential python3-pip \
 && apt-get -y autoremove \
 && apt-get -y clean

COPY sz_rabbit_consumer.py /app/

RUN curl -X GET \
      --output /tmp/instantclient-basic-linuxx64.zip \
      https://download.oracle.com/otn_software/linux/instantclient/instantclient-basic-linuxx64.zip

RUN unzip /tmp/instantclient-basic-linuxx64.zip -d /app

ENV PYTHONPATH=/opt/senzing/g2/sdk/python:/app
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/app/instantclient_21_9/

USER 1001

WORKDIR /app
ENTRYPOINT ["/app/sz_rabbit_consumer.py"]

