#!/usr/bin/env python

import os, sys
import traceback
import json
import time
import random
import datetime
from contextlib import contextmanager
from contextlib import ContextDecorator
from snap import common
from mercury import journaling as jrnl
from bx_services import S3Key


class UnrecognizedJobType(Exception):
    def __init__(self, job_tag):
        super().__init__(self, 'Could not determine type for job %s' % job_tag)


class NoHandlerRegisteredForJobType(Exception):
    def __init__(self, job_type):
        super().__init__(self, 'No handler function registered for job type "%s"' % job_type)


class start_timer(ContextDecorator):
    def __init__(self):
        super().__init__()
        self.start_time = None 

    def __enter__(self):
        self.start_time = time.time()
        return self

    def poll_seconds(self):
        return int(time.time() - self.start_time)

    def reset(self):
        self.start_time = time.time()

    def __exit__(self, *exc):
        return False


def determine_job_type(jsondata):
    job_tag = jsondata['job_tag']
    raise UnrecognizedJobType(job_tag)


def get_handler_for_job_type(job_type):
    handler_func = JOB_DISPATCH_TABLE.get(job_type)
    if not handler_func:
        raise NoHandlerRegisteredForJobType(job_type)

    return handler_func


def arbitrate(job_post_data, bid_data, service_registry):
    # Decide which bidder gets assigned a job, using a simple random selector. This is only for the proof of concept;
    # we will upgrade to smarter (and user-pluggable) arbitration methods once we shake the system out.

    couriers = bid_data['couriers']
    random.seed(time.time())
    index = random.randrange(0, len(couriers))

    return couriers[index]


def handle_job_posted(job_post_data, service_registry):
    '''when a job is posted, broadcast the notice via SMS to all available couriers,
    who may then "bid" to accept the job. The current JSON format for a job posting is:
    {
        "job_data": {
            # <job_data DB table fields>
        },
        "available_couriers": [],
        "bid_window": {
            "limit_type": "time_seconds" | "num_bids"
            "limit": <limit> 
        }
    }
    
    '''
    job_tag = job_post_data['job_data']['job_tag']

    # text the tag of the available job to all couriers who were in the on-call roster
    # at the time the job was posted
    sms_service = service_registry.lookup('sms')
    api_service = service_registry.lookup('job_mgr_api')

    for courier_record in job_post_data['available_couriers']:
        sms_service.send_sms(courier_record['mobile_number'], job_tag)

    bidding_threshold_type = job_post_data['bid_window']['limit_type']
    bidding_threshold = int(job_post_data['bid_window']['limit'])
    bid_data = None

    if bidding_threshold_type == 'num_bids':
        poll_interval = 2

        while num_bids < bidding_threshold:
            print('#---- Polling every %d seconds until we receive at least %d bids...' % (poll_interval, bidding_threshold))
            time.sleep(poll_interval)

            response = api_service.get_active_job_bids(job_tag)
            bid_data = response.json()['data']
            print(common.jsonpretty(bid_data))
            num_bids = len(bid_data['couriers'])

            # TODO: we may need an overall timeout, so that we don't stay in here forever

    elif bidding_threshold_type == 'time_seconds':
        while True:
            # now retrieve the couriers who have bid on this job
            print('#---- Waiting %d seconds for bidding window to close...' % bidding_threshold)
            time.sleep(bidding_threshold)

            response = api_service.get_active_job_bids(job_tag)
            bid_data = response.json()['data']
            print(common.jsonpretty(bid_data))

            num_bids = len(bid_data['couriers'])
            if not num_bids:
                # TODO: pluggable policy as to whether we should close the window if no one bids
                print('#---- The %s second bidding window closed with no job bids. Retrying.' % bidding_threshold)
                continue
            
            break


    assignee = arbitrate(job_post_data, bid_data, service_registry)

    job_assigned_reply_lines = [
        '*********',
        'Hello %s, you have been assigned the delivery job with tag %s.' % (assignee['first_name'], job_tag),
        'Text the job tag, space, and "det" to see job details.',
        'Godspeed!',
        '*********'
    ]
    msg = '\n'.join(job_assigned_reply_lines)
    sms_service.send_sms(assignee['mobile_number'], msg)

            
                

def handle_job_assigned(jsondata, service_registry):
    '''after some number of couriers have bid to accept a job (or after some 
    time window has elapsed),the job is assigned to one of them based on 
    user-defined arbitration logic. The job assignment is a systemwide event, 
    so we handle it here by 
    
    (a) notifying, via SMS, the courier to whom the job was assigned
    (b) notifying, via SMS, the couriers *not* assigned
    '''

    # TODO: notification logic
    pass


S3_EVENT_DISPATCH_TABLE = {
    'posted': handle_job_posted,
    'assigned': handle_job_assigned
}

def msg_handler(message, receipt_handle, service_registry):

    #service_registry = common.ServiceObjectRegistry(service_tbl)
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
            jsondata = s3_svc.download_json(bucket_name, object_key)
            print('### JSON payload data:')
            print(common.jsonpretty(jsondata))

            # we use the name of the top-level S3 "folder" to select the action to perform,
            # by keying into the dispatch table
            channel_id = object_key.split('/')[0]
            handler = S3_EVENT_DISPATCH_TABLE.get(channel_id)
            if not handler:
                raise Exception('no handler registered for S3 upload events to bucket %s with key %s' % (bucket_name, object_key))

            handler(jsondata, service_registry)

        except Exception as err:
            print('Error handling JSON job data from URI %s.' % s3key.uri)
            print(err)
            traceback.print_exc(file=sys.stdout)
            return

        """
        # to time a block of code:

        time_log = jrnl.TimeLog()
        timer_label = 'job_handler_exec_time: %s' % job_tag

        with jrnl.stopwatch(timer_label, time_log):
            <code block>

        print(time_log.readout)

        """