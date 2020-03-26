#!/usr/bin/env python


import uuid
import json
from snap import snap
from snap import core
from snap.loggers import transform_logger as log
from sqlalchemy.sql import text
import git
import constants as const

'''
TODO: if a job's core information changes AFTER the job has been accepted, auto-generate message(s) for the courier
containing the updated information

TODO: figure out credentials and groups (add Sasha)
--aws secrets manager?

TODO: if a courier accepts only one phase of an advertised job:

1. continue broadcasting with an updated notice that only the remaining phase is required, and 
2. update the job's status to "accepted_partial". 

Once all phases are accepted, ensure that the multiple assigned couriers are notified of their respective
assignments.

'''


def copy_fields_from(source_dict, *fields):
    output_dict = {}
    for field in fields:
        output_dict[field] = source_dict.get(field)
    
    return output_dict


def generate_job_tag(name):
    id = uuid.uuid4()
    raw_tag = '%s-%s' % (name, id)
    return raw_tag.replace('_', '-')


class ObjectFactory(object):
    @classmethod
    def create_courier(cls, db_svc, **kwargs):
        Courier = db_svc.Base.classes.couriers
        return Courier(**kwargs)

    @classmethod
    def create_courier_transport_method(cls, db_svc, **kwargs):
        CourierTransportMethod = db_svc.Base.classes.courier_transport_methods
        return CourierTransportMethod(**kwargs)

    @classmethod
    def create_courier_borough(cls, db_svc, **kwargs):
        CourierBorough = db_svc.Base.classes.courier_boroughs
        return CourierBorough(**kwargs)

    @classmethod
    def create_client(cls, db_svc, **kwargs):
        Client = db_svc.Base.classes.clients
        return Client(**kwargs)

    @classmethod
    def create_job(cls, db_svc, **kwargs):
        Job = db_svc.Base.classes.job_data
        return Job(**kwargs)


def lookup_transport_method_ids(name_array, session, db_svc):
    TransportMethod = db_svc.Base.classes.transport_methods
    ids = []
    for name in name_array:
        # one() will throw an exception if no match
        result = session.query(TransportMethod).filter(TransportMethod.value == name).one()
        ids.append(result.id)

    return ids


def lookup_borough_ids(name_array, session, db_svc):
    Borough = db_svc.Base.classes.boroughs
    ids = []
    for name in name_array:
        result = session.query(Borough).filter(Borough.value == name).one()
        ids.append(result.id)

    return ids


def lookup_payment_method_id(name, session, db_svc):
    PaymentMethod = db_svc.Base.classes.lookup_payment_methods
    method = session.query(PaymentMethod).filter(PaymentMethod.value == name).one()
    return method.id


def prepare_courier_record(input_data, session, db_svc):
    output_record = copy_fields_from(input_data, 'first_name', 'last_name', 'mobile_number', 'email')
    output_record['duty_status'] = 0    # 0 is inactive, 1 is active
    return output_record


def prepare_job_record(input_data, session, db_svc):
    output_record = copy_fields_from(input_data,
                                    'client_id',
                                    'delivery_address',
                                    'delivery_borough',
                                    'delivery_zip',
                                    'delivery_neighborhood',
                                    'pickup_address',
                                    'pickup_borough',
                                    'pickup_neighborhood',
                                    'pickup_zip',
                                    'items',
                                    'delivery_window_open',
                                    'delivery_window_close')

    borough_tag = input_data['delivery_borough'].lstrip().rstrip().lower().replace(' ', '_')
    output_record['payment_method'] = lookup_payment_method_id(input_data['payment_method'], session, db_svc)
    output_record['job_tag'] = generate_job_tag('bxlog_%s_%s' % (borough_tag, input_data['delivery_zip']))
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
    repo = git.Repo(search_parent_directories=True)
    sha = repo.head.object.hexsha
    return core.TransformStatus(ok_status('The BXLOGIC web listener is alive.', commit_id=sha))


def new_courier_func(input_data, service_objects, **kwargs):

    db_svc = service_objects.lookup('postgres')
    courier_id = None

    with db_svc.txn_scope() as session:

        methods = [m.lstrip().rstrip() for m in input_data['transport_methods'].split(',')]
        transport_method_ids = lookup_transport_method_ids(methods, session, db_svc)

        boroughs = [b.lstrip().rstrip() for b in input_data['boroughs'].split(',')]
        borough_ids = lookup_borough_ids(boroughs, session, db_svc)

        raw_record = prepare_courier_record(input_data, session, db_svc)
        courier = ObjectFactory.create_courier(db_svc, **raw_record)
        session.add(courier)
        session.flush()
        courier_id = courier.id

        for id in transport_method_ids:
            session.add(ObjectFactory.create_courier_transport_method(db_svc,
                                                                      courier_id=courier.id,
                                                                      transport_method_id=id))
        for id in borough_ids:
            session.add(ObjectFactory.create_courier_borough(db_svc,
                                                             courier_id=courier.id,
                                                             borough_id=id))

    return core.TransformStatus(ok_status('new Courier created', id=courier_id))


def new_job_func(input_data, service_objects, **kwargs):
    db_svc = service_objects.lookup('postgres')
    job_id = None
    raw_record = None
    with db_svc.txn_scope() as session:
        raw_record = prepare_job_record(input_data, session, db_svc)
        job = ObjectFactory.create_job(db_svc, **raw_record)
        session.add(job)
        session.flush()
        job_id = job.id
    
    raw_record['id'] = job_id

    pipeline_svc = service_objects.lookup('job_pipeline')
    s3_svc = service_objects.lookup('s3')
    pipeline_svc.post_job_notice(raw_record['job_tag'],
                                 s3_svc, 
                                 message='testing job service',
                                 available_couriers=['9174176968'])

    return core.TransformStatus(ok_status('new Job created', data=raw_record))


def new_client_func(input_data, service_objects, **kwargs):
    db_svc = service_objects.lookup('postgres')
    client_id = None
    with db_svc.txn_scope() as session:
        client = ObjectFactory.create_client(db_svc, **input_data)
        session.add(client)
        session.flush()
        client_id = client.id

    input_data['id'] = client_id
    return core.TransformStatus(ok_status('new Client created', data=input_data))


def sms_responder_func(input_data, service_objects, **kwargs):
    print('###------ SMS payload:')
    
    source_number = input_data['From']
    message_body = input_data['Body']

    print('###------ Received message "%s" from mobile number [%s].' % (message_body, source_number))

    mobile_number = normalize_mobile_number(source_number)


    return core.TransformStatus(ok_status('SMS event received'))




