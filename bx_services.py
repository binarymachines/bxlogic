#!/usr/bin/env python

import os, sys
import yaml
from snap import common
from collections import namedtuple
from contextlib import contextmanager
import datetime
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


POSTGRESQL_SVC_PARAM_NAMES = [
    'host',
    'database',
    'schema',
    'username',
    'password'
]

SuspensionEvent = namedtuple('SuspensionEvent', 'user date')

MONTH_INDEX = 0
DAY_INDEX = 1
YEAR_INDEX = 2


def parse_date(date_string):
    tokens = [int(token) for token in date_string.split("/")]
    return datetime.date(tokens[YEAR_INDEX],
                         tokens[MONTH_INDEX],
                         tokens[DAY_INDEX])


class BusinessEventDB(object):
    def __init__(self, **kwargs):

        location = kwargs['location']
        datafile = kwargs['datafile']
        datapath = os.path.join(location, datafile)

        self.db = {}
        self.data = None
        with open(datapath, 'r') as f:
            self.data = yaml.safe_load(f)

        for s in self.data['suspension_events']:
            self.db[s['user']] = SuspensionEvent(user=s['user'], date=parse_date(s['date']))

        #print(self.db, file=sys.stderr)


    def find_suspension_event(self, username):
        return self.db.get(username)


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
        while not connected and retries < 3:
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
                raise Exception(s3_auth_error_mesage)           
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
    
    '''
    def download_object(self, bucket_name, s3_key_string):
        #s3_object_key = S3Key(s3_key_string)
        local_filename = os.path.join(self.local_tmp_path, s3_object_key.object_name)
        with open(local_filename, "wb") as f:
            self.s3client.download_fileobj(bucket_name, s3_object_key.full_name, f)
        return local_filename
    '''

    def download_json(self, bucket_name, s3_key_string):
        #s3_object_key = S3Key(s3_key_string)

        obj = self.s3client.get_object(Bucket=bucket_name, Key=s3_key_string)
        return json.loads(obj['Body'].read().decode('utf-8'))
