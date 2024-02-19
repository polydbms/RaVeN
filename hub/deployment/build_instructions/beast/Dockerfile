FROM bitnami/spark:3.5.0

USER root

RUN apt-get -y update --fix-missing && \
    apt-get -y install wget

ENV BEAST_VERSION 0.10.0

RUN wget https://bitbucket.org/bdlabucr/beast/downloads/beast-${BEAST_VERSION}-bin.tar.gz && \
    tar -f beast-${BEAST_VERSION}-bin.tar.gz -C $SPARK_HOME -xvz
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
