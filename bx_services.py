#!/usr/bin/env python

import os, sys
import json
from collections import namedtuple
from contextlib import contextmanager
import datetime

from snap import common

import yaml
import requests
import boto3
import sqlalchemy as sqla
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy_utils import UUIDType

from twilio.rest import Client


POSTGRESQL_SVC_PARAM_NAMES = [
    'host',
    'database',
    'schema',
    'username',
    'password'
]

PIPELINE_SVC_PARAM_NAMES = [
    'job_bucket_name',
    'posted_jobs_folder',
    'accepted_jobs_folder'
]

MONTH_INDEX = 0
DAY_INDEX = 1
YEAR_INDEX = 2


def parse_date(date_string):
    tokens = [int(token) for token in date_string.split("/")]
    return datetime.date(tokens[YEAR_INDEX],
                         tokens[MONTH_INDEX],
                         tokens[DAY_INDEX])


ALLOWED_BIDDING_LIMIT_TYPES = ['time_seconds', 'num_bids']

class JobPipelineService(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader(*PIPELINE_SVC_PARAM_NAMES)
        kwreader.read(**kwargs)
        self.job_bucket_name = kwargs['job_bucket_name']
        self.posted_jobs_folder = kwargs['posted_jobs_folder']
        self.accepted_jobs_folder = kwargs['accepted_jobs_folder']
        self.bid_window_limit_type = kwargs['bid_window_limit_type']

        if self.bid_window_limit_type not in ALLOWED_BIDDING_LIMIT_TYPES:
            raise Exception('Invalid bidding limit type %s. Allowed types are %s.' % 
                            (self.bid_window_limit_type, ALLOWED_BIDDING_LIMIT_TYPES))

        self.bid_window_limit = int(kwargs['bid_window_limit'])


    def post_job_notice(self, tag, s3_svc, **kwargs):
        job_request_s3_key = '%s/%s.json' % (self.posted_jobs_folder, tag)
        payload = kwargs
        payload['bid_window'] = {
            'limit_type': self.bid_window_limit_type,
            'limit': self.bid_window_limit
        }
        s3_svc.upload_json(payload, self.job_bucket_name, job_request_s3_key)


    def post_job_bid(self, tag, courier_id, s3_svc, **kwargs):
        job_request_s3_key = '%s/%s.json' % (self.posted_jobs_folder, tag)
        payload = kwargs
        s3_svc.upload_json(payload, self.job_bucket_name, job_request_s3_key)
        

class SMSService(object):
    def __init__(self, **kwargs):
        account_sid = kwargs['account_sid']
        auth_token = kwargs['auth_token']
        self.source_number = kwargs['source_mobile_number']

        if not account_sid:
            raise Exception('Missing Twilio account SID var.')

        if not auth_token:
            raise Exception('Missing Twilio auth token var.')

        self.client = Client(account_sid, auth_token)


    def send_sms(self, mobile_number, message):
        print('### sending message body via SMS from [%s] to [%s] :' % (self.source_number, mobile_number))
        print(message)

        message = self.client.messages.create(
            to='+1%s' % mobile_number,
            from_='+1%s' % self.source_number,
            body=message
        )

        return message.sid


class PostgreSQLService(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader(*POSTGRESQL_SVC_PARAM_NAMES)
        kwreader.read(**kwargs)

        self.db_name = kwargs['database']
        self.host = kwargs['host']
        self.port = int(kwargs.get('port', 5432))
        self.username = kwargs['username']
        self.password = kwargs['password']        
        self.schema = kwargs['schema']
        self.max_connect_retries = int(kwargs.get('max_connect_retries') or 3)
        self.metadata = None
        self.engine = None
        self.session_factory = None
        self.Base = None
        self.url = None
                
        url_template = '{db_type}://{user}:{passwd}@{host}/{database}'
        db_url = url_template.format(db_type='postgresql+psycopg2',
                                     user=self.username,
                                     passwd=self.password,
                                     host=self.host,
                                     port=self.port,
                                     database=self.db_name)
        
        retries = 0
        connected = False
        while not connected and retries < self.max_connect_retries:
            try:
                self.engine = sqla.create_engine(db_url, echo=False)
                self.metadata = MetaData(schema=self.schema)
                self.Base = automap_base(bind=self.engine, metadata=self.metadata)
                self.Base.prepare(self.engine, reflect=True)
                self.metadata.reflect(bind=self.engine)
                self.session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

                # this is required. See comment in SimpleRedshiftService 
                connection = self.engine.connect()                
                connection.close()
                connected = True
                print('### Connected to PostgreSQL DB.', file=sys.stderr)
                self.url = db_url

            except Exception as err:
                print(err, file=sys.stderr)
                print(err.__class__.__name__, file=sys.stderr)
                print(err.__dict__, file=sys.stderr)
                time.sleep(1)
                retries += 1
            
        if not connected:
            raise Exception('!!! Unable to connect to PostgreSQL db on host %s at port %s.' % 
                            (self.host, self.port))

    @contextmanager
    def txn_scope(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


    @contextmanager    
    def connect(self):
        connection = self.engine.connect()
        try:
            yield connection
        finally:
            connection.close()


class S3Key(object):
    def __init__(self, bucket_name, s3_object_path):
        self.bucket = bucket_name
        self.folder_path = self.extract_folder_path(s3_object_path)
        self.object_name = self.extract_object_name(s3_object_path)
        self.full_name = s3_object_path

    def extract_folder_path(self, s3_key_string):
        if s3_key_string.find('/') == -1:
            return ''
        key_tokens = s3_key_string.split('/')
        return '/'.join(key_tokens[0:-1])

    def extract_object_name(self, s3_key_string):
        if s3_key_string.find('/') == -1:
            return s3_key_string
        return s3_key_string.split('/')[-1]

    def __str__(self):
        return self.full_name

    @property
    def uri(self):
	    return os.path.join('s3://', self.bucket, self.full_name)


class S3Service(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('local_temp_path', 'region')
        kwreader.read(**kwargs)

        self.local_tmp_path = kwreader.get_value('local_temp_path')
        self.region = kwreader.get_value('region')
        self.s3session = None
        self.aws_access_key_id = None
        self.aws_secret_access_key = None

        # we set this to True if we are initializing this object from inside an AWS Lambda,
        # because in that case we do not require the aws credential parameters to be set.
        # The default is False, which is what we want when we are creating this object
        # in a normal (non-AWS-Lambda) execution context: clients must pass in credentials.
        should_authenticate_via_iam = kwargs.get('auth_via_iam', False)

        if not should_authenticate_via_iam:
            print("NOT authenticating via IAM. Setting credentials now.", file=sys.stderr)            
            self.aws_access_key_id = kwargs.get('aws_key_id')
            self.aws_secret_access_key = kwargs.get('aws_secret_key')
            if not self.aws_secret_access_key or not self.aws_access_key_id:
                raise Exception('S3 authorization failed. Please check your credentials.')     

            self.s3client = boto3.client('s3',
                                         aws_access_key_id=self.aws_access_key_id,
                                         aws_secret_access_key=self.aws_secret_access_key)
        else:
            self.s3client = boto3.client('s3', region_name=self.region)
 

    def upload_object(self, local_filename, bucket_name, bucket_path=None):
        s3_path = None
        with open(local_filename, 'rb') as data:
            base_filename = os.path.basename(local_filename)
            if bucket_path:
                s3_path = os.path.join(bucket_path, base_filename)
            else:
                s3_path = base_filename
            self.s3client.upload_fileobj(data, bucket_name, s3_path)
        return S3Key(bucket_name, s3_path)


    def upload_json(self, data_dict, bucket_name, bucket_path):
        binary_data = bytes(json.dumps(data_dict), 'utf-8')
        self.s3client.put_object(Body=binary_data, 
                                 Bucket=bucket_name, 
                                 Key=bucket_path)


    def upload_bytes(self, bytes_obj, bucket_name, bucket_path):
        s3_key = bucket_path
        self.s3client.put_object(Body=bytes_obj, Bucket=bucket_name, Key=s3_key)
        return s3_key
    

    def download_json(self, bucket_name, s3_key_string):
        obj = self.s3client.get_object(Bucket=bucket_name, Key=s3_key_string)
        return json.loads(obj['Body'].read().decode('utf-8'))



class APIError(Exception):
    def __init__(self, url, method, status_code):
        super().__init__(self, 
                       'Error sending %s request to URL %s: status code %s' % (method, url, status_code))


APIEndpoint = namedtuple('APIEndpoint', 'host port path method')

class BXLogicAPIService(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('host', 'port')
        kwreader.read(**kwargs)
        self.hostname = kwreader.get_value('host')
        self.port = int(kwreader.get_value('port'))

        self.poll_job = APIEndpoint(host=self.hostname, port=self.port, path='job', method='GET')
        self.update_job_status = APIEndpoint(host=self.hostname, port=self.port, path='jobstatus', method='POST')
        self.update_job_log = APIEndpoint(host=self.hostname, port=self.port, path='joblog', method='POST')
        self.poll_job_bids = APIEndpoint(host=self.hostname, port=self.port, path='bidders', method='GET')
        self.couriers = APIEndpoint(host=self.hostname, port=self.port, path='couriers', method='GET')


    def endpoint_url(self, api_endpoint, **kwargs):
        if kwargs.get('ssl') == True:
            scheme = 'https'
        else:
            scheme = 'http'
            
        url = '{scheme}://{host}:{port}'.format(scheme=scheme, host=api_endpoint.host, port=api_endpoint.port)
        return os.path.join(url, api_endpoint.path)


    def _call_endpoint(self, endpoint, payload, **kwargs):        
        url_path = self.endpoint_url(endpoint, **kwargs)
        if endpoint.method == 'GET':                        
            print('calling endpoint %s using GET with payload %s...' % (url_path, payload))
            return requests.get(url_path, params=payload)
        if endpoint.method == 'POST':
            print('calling endpoint %s using POST with payload %s...' % (url_path, payload))
            return requests.post(url_path, data=payload)


    def get_active_job_bids(self, job_tag, **kwargs):
        payload = {'job_tag': job_tag}
        response = self._call_endpoint(self.poll_job_bids,
                                       payload, 
                                       **kwargs)
        return response


    def get_available_couriers(self, **kwargs):
        payload = { 'status': 1 }
        response = self._call_endpoint(self.couriers,
                                       payload, 
                                       **kwargs)
        return response


    def notify_job_completed(self, job_tag, **kwargs):
        print('### signaling completion for job tag %s' % job_tag)
        payload = {'job_tag': job_tag, 'status': 'completed'}
        response = self._call_endpoint(self.update_job_status,
                                       payload)
        if not response:
            raise APIError(self.endpoint_url(self.update_job_status),
                           self.update_job_status.method,
                           response.status_code)


    def notify_job_canceled(self, job_tag, **kwargs):
        print('### --- signaling cancellation for job tag %s' % job_tag)
        response = self._call_endpoint(self.update_job_status,
                                       {'job_tag': job_tag,
                                        'status': 'canceled'})
        if not response:
            raise APIError(self.endpoint_url(self.update_job_status),
                           self.update_job_status.method,
                           response.status_code)

    
    def send_log_msg(self, job_tag, raw_message):
        print('### sending message to joblog for tag %s' % job_tag)
        message = urllib.parse.quote(raw_message)
        response = self._call_endpoint(self.update_job_log,
                                       {'job_tag': job_tag,
                                        'log_message': message})

        # fire and forget -- don't bother raising an exception if this doesn't succeed   

