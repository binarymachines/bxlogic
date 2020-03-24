#!/usr/bin/env python

import json

def decode_json(http_request):
    result =  http_request.get_json(silent=True)
    return result or {}


