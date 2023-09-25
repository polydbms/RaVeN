FROM spark-bookworm

USER root

#RUN ln -s /usr/lib/x86_64-linux-gnu/libffi.so.8 /usr/lib/x86_64-linux-gnu/libffi.so.7
#RUN ln -s /usr/libexec/gcc/x86_64-redhat-linux/4.4.4/cc1plus /usr/local/bin/

#RUN apt-get -y update --fix-missing && \
#    apt-get -y install software-properties-common
#
#RUN add-apt-repository ppa:ubuntugis/ppa && \
#RUN apt-get -y update --fix-missing && \
#    apt-get -y install build-essential ca-certificates software-properties-common


#RUN apt-get -y install wget && \
#    wget http://ftp.de.debian.org/debian/pool/main/libf/libffi/libffi7_3.3-6_amd64.deb -O libffi7.deb && \
#    apt-get install -y -f ./libffi7.deb

#RUN #apt-get -y install software-properties-common

#RUN add-apt-repository ppa:ubuntugis/ppa && \
RUN apt-get -y update && \
    apt-get -y install -V libgdal-dev build-essential

RUN apt-get -y install wget curl && \
    wget http://nz2.archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2.19_amd64.deb -O libssl1.1.deb && \
    wget http://ftp.de.debian.org/debian/pool/main/libf/libffi/libffi7_3.3-6_amd64.deb -O libffi7.deb && \
    dpkg -i libssl1.1.deb && \
    dpkg -i libffi7.deb

COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

RUN geopyspark install-jar
