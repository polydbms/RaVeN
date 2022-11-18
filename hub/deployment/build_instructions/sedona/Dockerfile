FROM bitnami/spark:3.3.1

USER root

RUN pip install geopandas apache-sedona py4j
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
