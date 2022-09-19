FROM osgeo/gdal:ubuntu-small-3.4.1 as requirements
RUN apt update && apt upgrade -y && apt install python3-pip -y
COPY requirements.txt ./requirements.txt
RUN pip install --user -r ./requirements.txt

FROM osgeo/gdal:ubuntu-small-3.4.1 as base
COPY --from=requirements /root/.local /root/.local
RUN apt update && apt upgrade -y && apt install openssh-client -y && ssh-keygen -b 2048 -t rsa -f ~/.ssh/lrv -q -N ""
WORKDIR /hub
CMD [ "/bin/bash" ]


FROM osgeo/gdal:ubuntu-small-3.4.1 as preprocess
WORKDIR /preprocess
COPY --from=requirements /root/.local /root/.local
COPY ./hub/evaluation/main.py hub/evaluation/main.py
COPY ./hub/utils/configurator.py hub/utils/configurator.py
COPY ./hub/utils/datalocation.py hub/utils/datalocation.py
COPY ./hub/utils/network.py hub/utils/network.py
COPY ./hub/utils/preprocess.py preprocess.py
CMD [ "/bin/bash" ]


FROM osgeo/gdal:ubuntu-small-3.4.1 as benchi
COPY --from=requirements /root/.local /root/.local
RUN apt update && apt upgrade -y && apt install openssh-client -y
WORKDIR /hub
ADD hub hub
ADD benchi.py benchi.py
CMD [ "/bin/bash" ]