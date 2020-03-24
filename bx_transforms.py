#!/usr/bin/env python

 
from snap import snap
from snap import core
import json
from snap.loggers import transform_logger as log
from sqlalchemy.sql import text
#import constants


class ObjectFactory(object):
    @classmethod
    def create_subscription_fact(cls, db_svc, **kwargs):
        SubscriptionFact = db_svc.Base.classes.fact_subscription_events
        return SubscriptionFact(**kwargs)


def ping_func(input_data, service_objects, **kwargs):
    return core.TransformStatus(json.dumps({'message': 'The BXLOGIC web listener is alive.'}))


def new_courier_func(input_data, service_objects, **kwargs):
    print(input_data)
    return core.TransformStatus(json.dumps({'status': 'ok', 'data': input_data}))


