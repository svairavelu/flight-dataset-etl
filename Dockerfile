FROM alpine:3.6

ENV PYTHONBUFFERED 1

RUN apk update \
  && apk --no-cache add python3 \
     py3-psycopg2 \
     wget \
  && wget --no-check-certificate https://bootstrap.pypa.io/get-pip.py \
  && python3 get-pip.py \
  && cd /usr/bin/ \
  && ln -s pydoc3 pydoc \
  && ln -s python3 python \
  && rm /get-pip.py \
  && apk del wget

RUN pip install --upgrade pip
RUN apk --update --no-cache add py-pip
RUN pip3 install boto3
COPY dataload.py /dataload.py

