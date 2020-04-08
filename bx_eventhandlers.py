#!/usr/bin/env python

import sys
import dateutil.parser
import traceback
import json
import time
import random
import datetime
from contextlib import ContextDecorator
from snap import common
#from mercury import journaling as jrnl
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


'''
def get_handler_for_job_type(job_type):
    handler_func = JOB_DISPATCH_TABLE.get(job_type)
    if not handler_func:
        raise NoHandlerRegisteredForJobType(job_type)

    return handler_func
'''


def arbitrate(bidder_list, service_registry):
    # Decide which bidder gets assigned a job, using a simple random selector.
    # This is only for the proof of concept; we will upgrade to smarter (and user-pluggable)
    # arbitration methods once we shake the system out.

    print('#####------- Arbitrating bid data:')
    print(common.jsonpretty(bidder_list))
    random.seed(time.time())
    index = random.randrange(0, len(bidder_list))
    return [bidder_list[index]]


def handle_system_scan(jsondata, service_registry):

    current_time = datetime.datetime.now()
    # scan ALL open bidding windows

    api_service = service_registry.lookup('job_mgr_api')
    response = api_service.get_open_bid_windows()
    bid_windows = response.json()['data']['bidding_windows']

    print('###----- Retrieved open bid windows from API endpoint:')
    print(bid_windows)

    # for each open window, see who has bid;
    for bwindow in bid_windows:

        job_tag = bwindow['job_tag']
        window_id = bwindow['bidding_window_id']

        if bwindow['policy']['limit_type'] == 'num_bids':
            print('++ Policy limit is %d bids.' % int(bwindow['policy']['limit']))

            json_bidder_data = api_service.get_active_job_bids(job_tag)
            bidding_users = json_bidder_data.json()['data']['bidders']
            num_bids = len(bidding_users)
            policy_limit_bids = int(bwindow['policy']['limit'])

            if num_bids >= policy_limit_bids:
                winners = arbitrate(bidding_users, service_registry)
                if len(winners):
                    print('!!!!!!!!!!!  WE HAVE A WINNER !!!!!!!!!!!!!!!!!!')
                    print(common.jsonpretty(winners))
                    api_service.award_job(window_id, winners)
                else:
                    print('### No winner determined in the arbitration round ending %s.' % current_time.isoformat())

        elif bwindow['policy']['limit_type'] == 'time_seconds':
            # see how long the window has been open;
            window_opened_at = dateutil.parser.parse(bwindow['open_ts'])
            window_open_duration = (current_time - window_opened_at).seconds
            policy_limit_seconds = int(bwindow['policy']['limit'])

            if window_open_duration >= policy_limit_seconds:
                json_bidder_data = api_service.get_active_job_bids(job_tag)
                bid_data = json_bidder_data.json()['data']['bidders']
                if len(bid_data):
                    winners = arbitrate(bid_data, service_registry)
                    if len(winners):
                        print('!!!!!!!!!!!  WE HAVE A WINNER !!!!!!!!!!!!!!!!!!')
                        api_service.award_job(window_id, winners)
                    else:
                        print('### No winner determined in the arbitration round ending %s.' % current_time.isoformat())
                else:
                    print('### No more bidders in this round.')

        else:
            # raise hell; we don't support that
            raise Exception('Unrecognized bidding window policy limit_type: %s' % bwindow['policy']['limit_type'])

        # use the policy data embedded in the bidding window to decide whether
        # to trigger (manual | automatic) arbitration


def handle_job_posted(job_post_data, service_registry):
    '''when a job is posted, broadcast the notice via SMS to all available couriers,
    who may then "bid" to accept the job. The current JSON format for a job posting is:
    {
        "job_data": {
            # <job_data DB table fields>
        },
        "bid_window": {
            "id": <window_id>,
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

    # get_available_couriers() should return:
    # { "data": "couriers": [{ <data> }, ...]
    response = api_service.get_available_couriers()
    couriers = response.json()['data']['couriers']

    for courier_record in couriers:
        sms_service.send_sms(courier_record['mobile_number'], job_tag)

    # done for now


S3_EVENT_DISPATCH_TABLE = {
    'posted': handle_job_posted,
    'scan': handle_system_scan
}


def scan_handler(message, receipt_handle, service_registry):
    print('### Inside top-level SCAN event handler function.')
    print("### message follows:")
    print(message)


def msg_handler(message, receipt_handle, service_registry):

    s3_svc = service_registry.lookup('s3')
    print('### Inside SQS message handler function.')
    print("### message follows:")
    print(common.jsonpretty(message))

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
