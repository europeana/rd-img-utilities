FROM ubuntu:20.04

WORKDIR /code

COPY . /code

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get -y install curl
RUN apt-get install -y git
RUN apt-get install sysvinit-utils

RUN apt-get install -y python3.9 \
    && ln -s /usr/bin/python3.9 /usr/bin/python3

RUN apt-get install -y python3-setuptools
RUN apt-get install -y python3-pip

RUN pip install -U pip

RUN pip install -r requirements.txt

COPY . /code



