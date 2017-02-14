FROM python:2.7

RUN pip install boto3 click pyopenssl

COPY dynamodb-bnr.py /usr/local/bin/dynamodb-bnr

ENTRYPOINT ["dynamodb-bnr"]
CMD ["--help"]
