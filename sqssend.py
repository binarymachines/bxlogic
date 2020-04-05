#!/usr/bin/env python

'''
Usage:
    sqssend --url <queue_url> --body <body> --attrs=<name:value.datatype>... [--delay <secs>]
    sqssend --url <queue_url> -s [--delay <secs>]
'''

# sqssend --url <queue_url> --body <body> [--delay <secs>]

import os
import json
import boto3
import docopt
from snap import common

STRING_TYPE = 'String'
NUMBER_TYPE = 'Number'


def parse_attributes(attr_string):
    data = {}
    tokens = attr_string.split(',')
    for t in tokens:
        nvpair = t.split(':')
        key = nvpair[0]
        raw_value = nvpair[1]

        delim_index = raw_value.find('%')

        value = raw_value[0:delim_index]
        datatype = raw_value[delim_index+1:]

        attr = {
            'DataType': datatype,
            'StringValue': value
        }

        data[key] = attr

    return data


def main(args):
    print(args)
    
    queue_url = args['<queue_url>']

    sendargs = {
        'QueueUrl': queue_url,
        'DelaySeconds': int(args.get('<delay>', 0)),
        'MessageAttributes': parse_attributes(args['--attrs'][0]),
        'MessageBody': args['<body>']
    }

    print(common.jsonpretty(sendargs))




if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)