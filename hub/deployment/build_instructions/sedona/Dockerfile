FROM bitnami/spark:3.5.0

USER root

RUN pip install geopandas apache-sedona py4j IPython
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
