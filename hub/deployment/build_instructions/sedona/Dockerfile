FROM bitnami/spark:3.5.0

USER root

RUN pip install geopandas apache-sedona==1.7.1 py4j IPython
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
