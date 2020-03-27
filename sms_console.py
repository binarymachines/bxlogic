#!/usr/bin/env python

'''
Usage:
    sms_console <configfile>
'''


import os
from snap import snap, common
import docopt

def main(args):

    configfile = args['<configfile>']
    yaml_config = common.read_config_file(configfile)
    service_tbl = snap.initialize_services(yaml_config)

    registry = common.ServiceObjectRegistry(service_tbl)

    sms_svc = registry.lookup('sms')
    sid = sms_svc.send_sms('9174176968', 'hello NET')
    print(sid)

if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)