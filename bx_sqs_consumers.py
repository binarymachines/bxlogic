#!/usr/bin/env python

import os, sys
import json
import datetime
from snap import common
from mercury import journaling as jrnl
from bx_services import S3Key
#from eos_watch_triggers import *


class UnrecognizedJobType(Exception):
    def __init__(self, job_tag):
        super().__init__(self, 'Could not determine type for job %s' % job_tag)


class NoHandlerRegisteredForJobType(Exception):
    def __init__(self, job_type):
        super().__init__(self, 'No handler function registered for job type "%s"' % job_type)


JOB_DISPATCH_TABLE = {
    
}

def determine_job_type(jsondata):
    job_tag = jsondata['job_tag']
    raise UnrecognizedJobType(job_tag)


def get_handler_for_job_type(job_type):
    handler_func = JOB_DISPATCH_TABLE.get(job_type)
    if not handler_func:
        raise NoHandlerRegisteredForJobType(job_type)

    return handler_func
    

def msg_handler(message, receipt_handle, service_tbl):

    service_registry = common.ServiceObjectRegistry(service_tbl)
    s3_svc = service_registry.lookup('s3')
    print('### Inside SQS message handler function.')
    print("### message follows:")

    # unpack SQS message to get notification about S3 file upload
    message_body_raw = message['Body']
    message_body = json.loads(message_body_raw)
    
    for record in message_body['Records']:
        s3_data = record.get('s3')
        if not record:
            continue

        bucket_name = s3_data['bucket']['name']
        object_key = s3_data['object']['key']
        # TODO: set a limit on file size?
        
        print('#--- received object upload notification [ bucket: %s, key: %s ]' % (bucket_name, object_key))

        s3key = S3Key(bucket_name, object_key)
        jsondata = None
        try:
            jobdata = s3_svc.download_json(bucket_name, object_key)
            print('### JSON payload data:')
            print(common.jsonpretty(jobdata))

            sms_service = service_registry.lookup('sms')
            for courier_num in jobdata['available_couriers']:
                sms_service.send_sms(courier_num, 'There is a new job in the system.')

        except Exception as err:
            print('Error handling JSON job data from URI %s.' % s3key.uri)
            print(err)
            return

        """
        jerry_svc = service_registry.lookup('job_mgr_api')
        job_tag = jsondata['job_tag']
        time_log = jrnl.TimeLog()
        timer_label = 'job_handler_exec_time: %s' % job_tag

        try:
            print('>>> starting data handling job with tag: [ %s ]' % job_tag, file=sys.stderr)

            jobtype = determine_job_type(jsondata)
            job_handler_func = get_handler_for_job_type(jobtype)
            
            with jrnl.stopwatch(timer_label, time_log):
                job_handler_func(jsondata, service_registry)
            print(time_log.readout)

        except Exception as err:
            jerry_svc.notify_job_failed(job_tag)
            print('!!! TOP LEVEL %s exception uncaught in event handler -- reporting FAILURE: %s' % (err.__class__.__name__, str(err)))
            print('!!! Notified job mgr of FAILURE for job tag %s' % job_tag, file=sys.stderr)
            print('!!! Exception of type %s thrown:' % err.__class__.__name__, file=sys.stderr)
            print(err, file=sys.stderr)
        """