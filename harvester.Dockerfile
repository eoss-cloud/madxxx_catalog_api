#
#
# building: docker build -t madxxx_harvester . -f harvester.Dockerfile
# using: docker run -it --rm -e AWS_DEFAULT_REGION=zzz -e AWS_ACCESS_KEY_ID=xxx -e AWS_SECRET_ACCESS_KEY=yyy madxxx_harvester list

# Pull base image.
FROM ubuntu:16.04

MAINTAINER Thilo Wehrmann <thilo.wehrmann@eoss.cloud>
LABEL Description="general Madxxx catalog docker image" Vendor="EOSS GmbH" Version="1.0"

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
  apt-get install -y python python-pip

RUN pip install --upgrade pip
RUN pip install distribute --upgrade

RUN apt-get install -y \
    software-properties-common libssl-dev libffi-dev \
    python-software-properties \
    build-essential htop \
    wget unzip vim python-psycopg2 gdal-bin python-gdal

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ADD . /eoss_catalogue_system
WORKDIR /eoss_catalogue_system
RUN pip install -r requirements.txt

#ENV EOSS_CATALOG_DB sqlite:////absolute/path/to/foo.db
ENV SENTINEL_USER bla
ENV SENTINEL_PASSWORD bla
ENV PYTHONPATH $PYTHONPATH:/eoss_catalogue_system/catalog

WORKDIR /eoss_catalogue_system
ENTRYPOINT ["python", "catalog/harvest/harvester.py"]