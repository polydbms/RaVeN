FROM osgeo/gdal:ubuntu-small-3.6.0 as requirements

RUN apt-get update && apt-get upgrade -y && apt-get install python3-pip -y
COPY requirements.txt ./requirements.txt
RUN pip install --user -r ./requirements.txt

#FROM osgeo/gdal:ubuntu-small-3.5.3 as base
#COPY --from=requirements /root/.local /root/.local
#RUN apt-get update && apt-get upgrade -y && apt-get install openssh-client -y && ssh-keygen -b 2048 -t rsa -f ~/.ssh/lrv -q -N ""
#WORKDIR /hub
#CMD [ "/bin/bash" ]


FROM osgeo/gdal:ubuntu-small-3.6.0 as preprocess
WORKDIR /preprocess
COPY --from=requirements /root/.local /root/.local
COPY ./configuration.py configuration.py
COPY ./hub/evaluation/measure_time.py hub/evaluation/measure_time.py
COPY ./hub/enums/rasterfiletype.py hub/enums/rasterfiletype.py
COPY ./hub/enums/vectorfiletype.py hub/enums/vectorfiletype.py
COPY ./hub/enums/vectorizationtype.py hub/enums/vectorizationtype.py
COPY ./hub/utils/configurator.py hub/utils/configurator.py
COPY ./hub/utils/capabilities.py hub/utils/capabilities.py
COPY ./hub/utils/network.py hub/utils/network.py
COPY ./hub/utils/system.py hub/utils/system.py
COPY ./hub/utils/preprocess.py preprocess.py
COPY ./capabilities.yaml capabilities.yaml
CMD [ "/bin/bash" ]


FROM osgeo/gdal:ubuntu-small-3.6.0 as benchi
COPY --from=requirements /root/.local /root/.local
RUN apt update && apt upgrade -y && apt install openssh-client -y
WORKDIR /hub
ADD capabilities.yaml capabilities.yaml
ADD configuration.py configuration.py
ADD hub hub
ADD benchi.py benchi.py
CMD [ "/bin/bash" ]