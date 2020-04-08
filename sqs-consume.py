#!/usr/bin/env python

'''
Usage:
    sqs-consume --config <configfile> --source <source_name> [--verbose]
    sqs-consume --version
'''

import sys
import time
import datetime
from multiprocessing import Process
import boto3
import docopt
from snap import snap, common
from sh import git

VERSION_NUM = '0.5.2'


def show_version():
    git_hash = git.describe('--always').lstrip().rstrip()
    return '%s[%s]' % (VERSION_NUM, git_hash)


def main(args):
    if args['--version']:
        print(show_version())
        return

    verbose_mode = False
    if args['--verbose']:
        verbose_mode = True

    configfile = args['<configfile>']
    yaml_config = common.read_config_file(configfile)

    source_name = args['<source_name>']
    if not yaml_config['sources'].get(source_name):
        raise Exception('No queue source "%s" defined. Please check your config file.')

    service_tbl = snap.initialize_services(yaml_config)
    service_registry = common.ServiceObjectRegistry(service_tbl)
    source_config = yaml_config['sources'][source_name]

    # Create SQS client
    region = source_config['region']
    polling_interval = int(source_config['polling_interval_seconds'])

    sqs = boto3.client('sqs', region_name=region)
    queue_url = common.load_config_var(source_config['queue_url'])
    msg_handler_name = source_config['handler']
    project_dir = common.load_config_var(yaml_config['globals']['project_home'])
    sys.path.append(project_dir)

    msg_handler_module = yaml_config['globals']['consumer_module']
    msg_handler_func = common.load_class(msg_handler_name, msg_handler_module)

    child_procs = []

    print('### initiating polling loop.')

    # loop forever
    while True:
        current_time = datetime.datetime.now().isoformat()
        if verbose_mode:
            print('### checking SQS queue %s for messages at %s...' % (queue_url, current_time), file=sys.stderr)

        # Receive message from SQS queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=30,
            # VisibilityTimeout (integer) -- The duration (in seconds) that the received messages
            # are hidden from subsequent retrieve requests after being retrieved by a ReceiveMessage request.
            WaitTimeSeconds=3
            # WaitTimeSeconds (integer) -- The duration (in seconds) for which the call waits for a message 
            # to arrive in the queue before returning.
            # If a message is available, the call returns sooner than WaitTimeSeconds . If no messages are available
            # and the wait time expires, the call returns successfully with an empty list of messages.
        )

        inbound_msgs = response.get('Messages') or []
        if not len(inbound_msgs):
            if verbose_mode:
                print('### No messages pending, sleeping %d seconds before re-try...' % polling_interval)

            time.sleep(polling_interval)
            continue

        for message in inbound_msgs:
            receipt_handle = message['ReceiptHandle']
            current_time = datetime.datetime.now().isoformat()
            print('### spawning message processor at %s...' % current_time, file=sys.stderr)

            try:
                # TODO: can we pickle a ServiceObjectRegistry?
                p = Process(target=msg_handler_func, args=(message, receipt_handle, service_registry))
                p.start()
                child_procs.append(p)
                print('### Queued message-handling subprocess with PID %s.' % p.pid, file=sys.stderr)

                # Delete received message from queue
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )

            except Exception as err:
                print('!!! Error processing message with receipt: %s' % receipt_handle, file=sys.stderr)
                print(err)


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)