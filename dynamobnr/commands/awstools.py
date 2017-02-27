# -*- coding: utf-8 -*-

import boto3


class AWSTools:
    def __init__(self, parameters=None):
        self.__parameters = parameters

        self.__aws_session = {}
        self.__aws_client = {}
        self.__aws_resource = {}
        self.__credentials = {
            'dynamodb': {
                'profile': self.__parameters.ddb_profile,
                'access_key': self.__parameters.ddb_access_key,
                'secret_key': self.__parameters.ddb_secret_key,
                'region': self.__parameters.ddb_region,
            },
            's3': {
                'profile': self.__parameters.s3_profile,
                'access_key': self.__parameters.s3_access_key,
                'secret_key': self.__parameters.s3_secret_key,
                'region': self.__parameters.s3_region,
            },
        }

    def get_session(self, service):
        if service not in self.__aws_session:
            if self.__credentials[service]['profile']:
                self.__aws_session[service] = boto3.Session(
                    profile_name=self.__credentials[service]['profile']
                )
            else:
                self.__aws_session[service] = boto3.Session(
                    region_name=self.__credentials[service]['region'],
                    aws_access_key_id=self.__credentials[service]['access_key'],
                    aws_secret_access_key=self.__credentials[service]['secret_key'],
                )
        return self.__aws_session[service]

    def get_client(self, service):
        if service not in self.__aws_client:
            self.__aws_client[service] = self.get_session(service).client(
                service_name=service
            )
        return self.__aws_client[service]

    def get_resource(self, service):
        if service not in self.__aws_resource:
            self.__aws_resource[service] = self.get_session(service).resource(
                service_name=service
            )
        return self.__aws_resource[service]

    def get_client_dynamodb(self):
        return self.get_client('dynamodb')

    def get_client_s3(self):
        return self.get_client('s3')
