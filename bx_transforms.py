#!/usr/bin/env python


import uuid
import json
from snap import snap
from snap import core
from snap.loggers import transform_logger as log
from sqlalchemy.sql import text
#import constants


def copy_fields_from(source_dict, *fields):
    output_dict = {}
    for field in fields:
        output_dict[field] = source_dict[field]
    
    return output_dict


def generate_job_tag(name):
    id = uuid.uuid4()
    raw_tag = '%s-%s' % (name, id)
    return raw_tag.replace('_', '-')


class ObjectFactory(object):
    @classmethod
    def create_courier(cls, db_svc, **kwargs):
        Courier = db_svc.Base.classes.fact_couriers
        return Courier(**kwargs)


def lookup_transport_method_ids(name_array, session, db_svc):
    TransportMethod = db_svc.Base.classes.transport_methods
    ids = []
    for name in name_array:
        query = session.query(TransportMethod).filter(TransportMethod.name == name)
        result = query.one()
        if result:
            ids.append(result.id)

    return ids


def lookup_borough_ids(name_array, session, db_svc):
    Borough = db_svc.Base.classes.boroughs
    ids = []
    for name in name_array:
        query = session.query(Borough).filter(Borough.name == name)
        result = query.one()
        if result:
            ids.append(result.id)

    return ids


def prepare_courier_record(input_data):
    output_record = copy_fields_from(input_data, 'first_name', 'last_name', 'mobile_number', 'email')
    output_record['duty_status'] = 0    # 0 is inactive, 1 is active
    return output_record


def ok_status(message, **kwargs):
    result = {
        'status': 'ok',
        'message': message
    }

    if kwargs:
        result['data'] = kwargs

    return json.dumps(result)


def ping_func(input_data, service_objects, **kwargs):
    return core.TransformStatus(ok_status('The BXLOGIC web listener is alive.'))


def new_courier_func(input_data, service_objects, **kwargs):

    db_svc = service_objects.lookup('postgres')
    courier_id = None

    transport_method_ids = lookup_transport_method_ids[input_data['transport_methods']]
    borough_ids = lookup_borough_ids[input_data['boroughs']]
    raw_record = prepare_courier_record(input_data)

    with db_svc.txn_scope() as session:
        courier = ObjectFactory.create_courier(raw_record)
        session.add(courier)
        session.flush()
        courier_id = courier.id

        for id in transport_method_ids:
            session.add(ObjectFactory.create_courier_transport_method(db_svc, courier_id, id))

        for id in borough_ids:
            session.add(ObjectFactory.create_courier_borough(db_svc, courier_id, id))

    return core.TransformStatus(ok_status('new Courier created', id=courier_id))


