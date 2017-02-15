FROM python:2.7

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY dynamodb-bnr.py /usr/local/bin/dynamodb-bnr
