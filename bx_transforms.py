#!/usr/bin/env python

import sys
import re
import uuid
import json
import traceback
import datetime
from urllib.parse import unquote_plus
from collections import namedtuple
from snap import snap, common
from snap import core
# from snap.loggers import transform_logger as log
# from sqlalchemy.sql import text
import git
# import constants as const
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import and_, or_

'''
TODO: if a job's core information changes AFTER the job has been accepted, auto-generate message(s) for the courier
containing the updated information

TODO: figure out credentials and groups (add Sasha)
--aws secrets manager?

TODO: phased jobs require the concept of "teams" == any jobdata pushed to S3 should have a "team_size" attribute 

TODO: if a courier accepts only one phase of an advertised job:

1. continue broadcasting with an updated notice that only the remaining phase is required, and 
2. update the job's status to "accepted_partial". 

Once all phases are accepted, ensure that the multiple assigned couriers are notified of their respective
assignments.

'''

SMSCommandSpec = namedtuple('SMSCommandSpec', 'command definition synonyms tag_required')
SMSGeneratorSpec = namedtuple('SMSGeneratorSpec', 'command definition specifier')
SMSPrefixSpec = namedtuple('SMSPrefixSpec', 'command definition defchar')

SYSTEM_ID = 'bxlog'
NETWORK_ID = 'ccr'

INTEGER_RX = re.compile(r'^[0-9]+$')
NEG_INTEGER_RX = re.compile(r'^-[0-9]+$')
RANGE_RX = re.compile(r'^[0-9]+\-[0-9]+$')

SMS_SYSTEM_COMMAND_SPECS = {
    'on': SMSCommandSpec(command='on', definition='Courier coming on duty', synonyms=[], tag_required=False),
    'off': SMSCommandSpec(command='off', definition='Courier going off duty', synonyms=[], tag_required=False),
    'bid': SMSCommandSpec(command='bid', definition='Bid for a delivery job', synonyms=[], tag_required=True),
    'bst': SMSCommandSpec(command='bst', definition='Look up the bidding status of this job', synonyms=[], tag_required=True),
    'acc': SMSCommandSpec(command='acc', definition='Accept a delivery job', synonyms=['ac'], tag_required=True),
    'dt': SMSCommandSpec(command='dt', definition='Detail (find out what a particular job entails)', synonyms=[], tag_required=True),
    'ert': SMSCommandSpec(command='er', definition='En route to either pick up or deliver for a job', synonyms=['er'], tag_required=True),
    'can': SMSCommandSpec(command='can', definition='Cancel (courier can no longer complete an accepted job)', synonyms=[], tag_required=True),
    'fin': SMSCommandSpec(command='fin', definition='Finished a delivery', synonyms=['f'], tag_required=True),
    '911': SMSCommandSpec(command='911', definition='Courier is having a problem and needs assistance', synonyms=[], tag_required=False),
    'hlp': SMSCommandSpec(command='hlp', definition='Display help prompts', synonyms=['?'], tag_required=False)
}

SMS_GENERATOR_COMMAND_SPECS = {
    'my': SMSGeneratorSpec(command='my', definition='List my pending (already accepted) jobs', specifier='.'),
    'opn': SMSGeneratorSpec(command='opn', definition='List open (available) jobs', specifier='.'),
    'awd': SMSGeneratorSpec(command='awd', definition='List my awarded (but not yet accepted) jobs', specifier='.')
}

SMS_PREFIX_COMMAND_SPECS = {
    '$': SMSPrefixSpec(command='$', definition='create a user-defined macro', defchar=':'),
    '@': SMSPrefixSpec(command='@', definition="send a message to a user's log via his or her handle", defchar=' '),
    '&': SMSPrefixSpec(command='&', definition='create a user handle for yourself', defchar=' ')
}

SMS_RESPONSES = {
    'assign_job': 'Thank you for responding -- job tag {tag} has been assigned to you.',
    'assigned_to_other': 'Another courier in the network responded first, but thank you for stepping up.'
}

REPLY_ASSIGN_JOB_TPL = 'Thank you for responding -- job tag {tag} has been assigned to you.'
REPLY_ASSIGNED_TO_OTHER = 'Another courier in the network responded first, but thank you for stepping up.'
REPLY_NOT_IN_NETWORK = "You have sent a control message to our logistics network, but you haven't been registered as one of our couriers."
REPLY_GET_INVOLVED_TPL = "If you'd like to become a courier, please send email to {contact_email} and someone will contact you."
REPLY_CMD_FORMAT = "You have texted a command that requires a job tag. Text the job tag, a space, and then the command."
REPLY_CMD_HELP_AVAILABLE = 'Text "help" to the target number to get a list of command strings and what they do.'
REPLY_INVALID_TAG_TPL = 'The job tag you have specified (%s) appears to be invalid.'


JOB_STATUS_BROADCAST = 0
JOB_STATUS_AWARDED = 1
JOB_STATUS_ACCEPTED = 3
JOB_STATUS_IN_PROGRESS = 4
JOB_STATUS_COMPLETED = 5


def generate_assign_job_reply(**kwargs):
    return REPLY_ASSIGN_JOB_TPL.format(**kwargs)


def generate_get_involved_reply(**kwargs):
    return REPLY_GET_INVOLVED_TPL.format(**kwargs)


def copy_fields_from(source_dict, *fields):
    output_dict = {}
    for field in fields:
        output_dict[field] = source_dict.get(field)

    return output_dict


def generate_job_tag(name):
    id = uuid.uuid4()
    raw_tag = '%s-%s' % (name, id)
    return raw_tag.replace('_', '-')


def is_valid_job_tag(tag):
    # TODO: get a regex going for this
    if not tag.startswith(SYSTEM_ID):
        return False

    if tag.find(' ') != -1:
        return False

    return True


def normalize_mobile_number(number_string):
    return number_string.lstrip('+').lstrip('1').replace('(', '').replace(')', '').replace('-', '').replace('.', '').replace(' ', '')


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
        kwargs['created_ts'] = datetime.datetime.now()
        return Job(**kwargs)

    @classmethod
    def create_job_status(cls, db_svc, **kwargs):
        JobStatus = db_svc.Base.classes.job_status
        return JobStatus(**kwargs)

    @classmethod
    def create_job_bid(cls, db_svc, **kwargs):
        JobBid = db_svc.Base.classes.job_bids
        kwargs['write_ts'] = datetime.datetime.now()
        return JobBid(**kwargs)

    @classmethod
    def create_bidding_window(cls, db_svc, **kwargs):
        policy = {            
            "limit_type": "time_seconds",
            "limit": 15,
        }

        BiddingWindow = db_svc.Base.classes.bidding_windows
        kwargs['open_ts'] = datetime.datetime.now()
        kwargs['policy'] = policy
        return BiddingWindow(**kwargs)

    @classmethod
    def create_macro(cls, db_svc, **kwargs):
        UserMacro = db_svc.Base.classes.user_macros
        return UserMacro(**kwargs)

    @classmethod
    def create_user_log(cls, db_svc, **kwargs):
        UserLog = db_svc.Base.classes.messages
        kwargs['created_ts'] = datetime.datetime.now()
        return UserLog(**kwargs)

    @classmethod
    def create_user_handle(cls, db_svc, **kwargs):
        UserHandle = db_svc.Base.classes.user_handle_maps
        return UserHandle(**kwargs)


    @classmethod
    def create_job_assignment(cls, db_svc, **kwargs):
        JobAssignment = db_svc.Base.classes.job_assignments
        return JobAssignment(**kwargs)


def lookup_transport_method_ids(name_array, session, db_svc):
    TransportMethod = db_svc.Base.classes.transport_methods
    ids = []
    for name in name_array:
        # one() will throw an exception if no match
        try:
            result = session.query(TransportMethod).filter(TransportMethod.value == name).one()
            ids.append(result.id)
        except NoResultFound:
            pass

    return ids


def lookup_borough_ids(name_array, session, db_svc):
    Borough = db_svc.Base.classes.boroughs
    ids = []
    for name in name_array:
        try:
            result = session.query(Borough).filter(Borough.value == name).one()
            ids.append(result.id)
        except NoResultFound:
            pass

    return ids


def lookup_payment_method_id(name, session, db_svc):
    PaymentMethod = db_svc.Base.classes.lookup_payment_methods
    try:    
        method = session.query(PaymentMethod).filter(PaymentMethod.value == name).one()
        return method.id
    except NoResultFound:
        return None


def lookup_couriers_by_status(status, session, db_svc):
    couriers = []
    Courier = db_svc.Base.classes.couriers
    resultset = session.query(Courier).filter(Courier.duty_status == status) # inactive is 0, active is 1 
    for record in resultset:
        couriers.append({
            'id': record.id,
            'first_name': record.first_name,
            'last_name': record.last_name,
            'mobile_number': record.mobile_number
            })
    return couriers


def lookup_job_data_by_tag(tag, session, db_svc):
    Jobdata = db_svc.Base.classes.job_data
    try:
        job = session.query(Jobdata).filter(and_(Jobdata.job_tag == tag,
                                                 Jobdata.deleted_ts == None)).one()
        return job
    except NoResultFound:
        return None


def lookup_courier_by_id(courier_id, session, db_svc):
    Courier = db_svc.Base.classes.couriers
    try:
        return session.query(Courier).filter(Courier.id == courier_id).one()
    except NoResultFound:
        return None


def lookup_bidding_window_by_id(window_id, session, db_svc):
    BiddingWindow = db_svc.Base.classes.bidding_windows
    try:
        return session.query(BiddingWindow).filter(BiddingWindow.id == window_id).one()
    except NoResultFound:
        return None


def lookup_open_bidding_window_by_job_tag(job_tag, session, db_svc):
    current_time = datetime.datetime.now()
    BiddingWindow = db_svc.Base.classes.bidding_windows
    try:
        return session.query(BiddingWindow).filter(and_(BiddingWindow.job_tag == job_tag,
                                                        BiddingWindow.open_ts <= current_time,
                                                        or_(BiddingWindow.close_ts == None,
                                                            BiddingWindow.close_ts > current_time))).one()
    except NoResultFound:
        return None


def lookup_open_bidding_windows(session, db_svc):
    current_time = datetime.datetime.now()
    BiddingWindow = db_svc.Base.classes.bidding_windows

    resultset = session.query(BiddingWindow).filter(and_(BiddingWindow.open_ts <= current_time,
                                                    or_(BiddingWindow.close_ts == None,
                                                        BiddingWindow.close_ts > current_time))).all()

    for record in resultset:
        yield record


def lookup_live_courier_handle(courier_id, session, db_svc):
    UserHandle = db_svc.Base.classes.user_handle_maps
    try:
        return session.query(UserHandle).filter(and_(UserHandle.user_id == courier_id,
                                                     UserHandle.expired_ts == None)).one()
    except NoResultFound:
        return None


def lookup_courier_by_handle(handle, session, db_svc):
    HandleMap = db_svc.Base.classes.user_handle_maps
    try:
        handle_map = session.query(HandleMap).filter(and_(HandleMap.handle == handle,
                                                          HandleMap.expired_ts == None)).one()

        return lookup_courier_by_id(handle_map.user_id, session, db_svc)

    except NoResultFound:
        return None


def lookup_current_job_status(job_tag, session, db_svc):
    JobStatus = db_svc.Base.classes.job_status
    try:
        return session.query(JobStatus).filter(and_(JobStatus.job_tag == job_tag,
                                                    JobStatus.expired_ts == None)).one()
    except NoResultFound:
        return None


def lookup_user_job_bid(job_tag, courier_id, session, db_svc):
    JobBid = db_svc.Base.classes.job_bids
    try:
        return session.query(JobBid).filter(and_(JobBid.job_tag == job_tag,
                                                 JobBid.courier_id == courier_id,
                                                 JobBid.expired_ts == None)).one()
    except NoResultFound:
        return None


def lookup_bid_by_id(bid_id, session, db_svc):
    JobBid = db_svc.Base.classes.job_bids
    try:
        return session.query(JobBid).filter(and_(JobBid.id == bid_id,
                                                 JobBid.expired_ts == None)).one()
    except NoResultFound:
        return None


def job_is_available(job_tag, session, db_svc):
    JobStatus = db_svc.Base.classes.job_status
    try:
        # status 0 is "broadcast" (available for bidding)
        session.query(JobStatus).filter(and_(JobStatus.expired_ts == None,
                                             JobStatus.job_tag == job_tag, 
                                             JobStatus.status == JOB_STATUS_BROADCAST)).one()
        return True
    except NoResultFound:
        return False


def job_is_awarded(job_tag, session, db_svc):
    JobStatus = db_svc.Base.classes.job_status
    try:
        # status 1 is "awarded" (bidding is complete and there is a winner)
        session.query(JobStatus).filter(and_(JobStatus.expired_ts == None,
                                             JobStatus.job_tag == job_tag, 
                                             JobStatus.status == JOB_STATUS_AWARDED)).one()
        return True
    except NoResultFound:
        return False


def job_belongs_to_courier(job_tag, courier_id, session, db_svc):
    JobAssignment = db_svc.Base.classes.job_assignments
    try:
        session.query(JobAssignment).filter(and_(JobAssignment.job_tag == job_tag,
                                                 JobAssignment.courier_id == courier_id)).one()
        return True
    except NoResultFound:
        return False


def list_awarded_jobs(courier_id, session, db_svc):
    jobs = []
    JobBid = db_svc.Base.classes.job_bids

    for bid in session.query(JobBid).filter(and_(JobBid.courier_id == courier_id,
                                                 JobBid.expired_ts == None,
                                                 JobBid.accepted_ts != None)).all():

        if job_is_awarded(bid.job_tag, session, db_svc):
            job = lookup_job_data_by_tag(bid.job_tag, session, db_svc)
            if not job:
                raise Exception('There is an orphan job in this dataset. Please contact your administrator.')
            else:
                jobs.append(job)

    return jobs

    '''
    # status 1 is "awarded" (granted to winning bidder, but not yet accepted)
    status_record = session.query(JobStatus).filter(and_(JobStatus.expired_ts == None,
                                                            JobStatus.job_tag == job_tag, 
                                                            JobStatus.status == 1)).one()
    '''


def list_accepted_jobs(courier_id, session, db_svc):
    JobAssignment = db_svc.Base.classes.job_assignments
    JobStatus = db_svc.Base.classes.job_status
    # resultset = session.query(JobAssignment).filter(JobAssignment.courier_id == dlg_context.courier.id).all()
    jobs = []
    for ja, stat in session.query(JobAssignment,
                                  JobStatus).filter(and_(JobStatus.job_tag == JobAssignment.job_tag,
                                                         JobStatus.status == JOB_STATUS_ACCEPTED,
                                                         JobStatus.expired_ts == None,
                                                         JobAssignment.courier_id == courier_id)).all():
        jobs.append(stat)

    return jobs


def list_available_jobs(session, db_svc):
    jobs = []
    JobStatus = db_svc.Base.classes.job_status
    resultset = session.query(JobStatus).filter(and_(JobStatus.expired_ts == None,
                                                     JobStatus.status == 0)).all()
    for record in resultset:
        jobs.append(record)

    return jobs


def prepare_courier_record(input_data, session, db_svc):
    output_record = copy_fields_from(input_data, 'first_name', 'last_name', 'email')
    output_record['mobile_number'] = normalize_mobile_number(input_data['mobile_number'])
    output_record['duty_status'] = 0    # 0 is inactive, 1 is active

    return output_record


def prepare_bid_window_record(input_data, session, db_svc):
    output_record = copy_fields_from(input_data, 'job_tag')
    job = lookup_job_data_by_tag(input_data['job_tag'], session, db_svc)
    if not job:
        raise Exception('no job found with tag %s.' % input_data['job_tag'])

    output_record['job_id'] = job.id
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


def exception_status(err, **kwargs):
    result = {
        'status': 'error',
        'error_type': err.__class__.__name__,
        'message': 'an exception of type %s occurred: %s' % (err.__class__.__name__, str(err))
    }

    if kwargs:
        result.update(**kwargs)

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
        try:
            raw_record = prepare_job_record(input_data, session, db_svc)
            job = ObjectFactory.create_job(db_svc, **raw_record)
            session.add(job)
            session.flush()
            job_id = job.id

            status_record = ObjectFactory.create_job_status(db_svc,
                                                            job_tag=raw_record['job_tag'],
                                                            status=0,
                                                            write_ts=datetime.datetime.now())
            session.add(status_record)

            # we've created a job, now open it up for bids
            bidding_window = ObjectFactory.create_bidding_window(db_svc,
                                                                 job_id=job_id,
                                                                 job_tag=raw_record['job_tag'])
            session.add(bidding_window)

            # now push the job notification to S3, which will broadcast the event
            # to the courier network
            raw_record['id'] = job_id
            pipeline_svc = service_objects.lookup('job_pipeline')
            s3_svc = service_objects.lookup('s3')
            pipeline_svc.post_job_notice(raw_record['job_tag'],
                                         s3_svc, 
                                         job_data=raw_record)

            return core.TransformStatus(ok_status('new Job created', data=raw_record))

        except Exception as err:
            session.rollback()
            return core.TransformStatus(exception_status(err), False, message=str(err))


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


def lookup_sms_command(cmd_string):
    for key, cmd_spec in SMS_SYSTEM_COMMAND_SPECS.items():
        if cmd_string == key:
            return cmd_spec
        if cmd_string in cmd_spec.synonyms:
            return cmd_spec

    return None


def lookup_generator_command(cmd_string):
    for key, cmd_spec in SMS_GENERATOR_COMMAND_SPECS.items():
        delimiter = cmd_spec.specifier
        if cmd_string.split(delimiter)[0] == key:
            return cmd_spec

    return None


class UnrecognizedSMSCommand(Exception):
    def __init__(self, cmd_string):
        super().__init__(self, 'Invalid SMS command %s' % cmd_string)


class IncompletePrefixCommand(Exception):
    def __init__(self, cmd_string):
        super().__init__(self, 'Incomplete prefix command %s' % cmd_string)


SystemCommand = namedtuple('SystemCommand', 'job_tag cmdspec modifiers')
GeneratorCommand = namedtuple('GeneratorCommand', 'cmd_string cmdspec modifiers')
PrefixCommand = namedtuple('PrefixCommand', 'mode name body cmdspec')

CommandInput = namedtuple('CommandInput', 'cmd_type cmd_object')  # command types: generator, syscommand, prefix


def parse_sms_message_body(raw_body):
    job_tag = None
    command_string = None
    modifiers = []

    # make sure there's no leading whitespace, then see what we've got
    body = unquote_plus(raw_body).lstrip().rstrip().lower()

    print('\n\n###__________ inside parse logic. Raw message body is:')
    print(body)
    print('###__________\n\n')

    if body.startswith('bxlog-'):
        # remove the URL encoded whitespace chars;
        # remove any trailing/leading space chars as well
        tokens = [token.lstrip().rstrip() for token in body.split(' ') if token]

        print('###------ message tokens: %s' % common.jsonpretty(tokens))

        job_tag = tokens[0]
        if len(tokens) == 2:
            command_string = tokens[1].lower()

        if len(tokens) > 2:
            command_string = tokens[1].lower()
            modifiers = tokens[2:]

        print('#--------- looking up system SMS command: %s...' % command_string)
        command_spec = lookup_sms_command(command_string)
        if command_spec:
            return CommandInput(cmd_type='syscommand', cmd_object=SystemCommand(job_tag=job_tag,
                                                                                cmdspec=command_spec,
                                                                                modifiers=modifiers))
        raise UnrecognizedSMSCommand(command_string)

    elif body[0] in SMS_PREFIX_COMMAND_SPECS.keys():
        prefix = body[0]
        prefix_spec = SMS_PREFIX_COMMAND_SPECS[prefix]
        print('### probable prefix command "%s". Body length is %d.' % (prefix, len(body)))
        if len(body) == 1:
            raise IncompletePrefixCommand(command_string)

        raw_body = body[1:].lower()
        defchar_index = raw_body.find(prefix_spec.defchar)
        # when a prefix command is issued containing a defchar, that is its "extended" mode
        if defchar_index > 0:            
            command_mode = 'extended'
            command_name = raw_body[0:defchar_index]
            command_data = raw_body[defchar_index+1:]
        # prefix commands issued without the defchar are running in "simple" mode
        else:
            command_mode = 'simple'
            command_name = raw_body
            command_data = None

        return CommandInput(cmd_type='prefix',
                            cmd_object=PrefixCommand(mode=command_mode,
                                                     name=command_name,
                                                     body=command_data,
                                                     cmdspec=prefix_spec))

    else:
        tokens = [token.lstrip().rstrip() for token in body.split(' ') if token]
        command_string = tokens[0].lower()
        modifiers = tokens[1:]

        # see if we received a generator 
        # (a command which generates a list or a slice of a list)
        command_spec = lookup_generator_command(command_string)
        if command_spec:
            print('###------------ detected GENERATOR-type command: %s' % command_string)            
            return CommandInput(cmd_type='generator',
                                cmd_object=GeneratorCommand(cmd_string=command_string,
                                                            cmdspec=command_spec,
                                                            modifiers=modifiers))

        # if we didn't find a generator, perhaps the user issued a regular sms comand                                             
        command_spec = lookup_sms_command(command_string)
        if command_spec:
            print('###------------ detected system command: %s' % command_string)
            return CommandInput(cmd_type='syscommand',
                                cmd_object=SystemCommand(job_tag=job_tag,
                                                         cmdspec=command_spec,
                                                         modifiers=modifiers))
    
        raise UnrecognizedSMSCommand(command_string)


def lookup_courier_by_mobile_number(mobile_number, session, db_svc):
    Courier = db_svc.Base.classes.couriers
    try:
        return session.query(Courier).filter(Courier.mobile_number == mobile_number).one()
    except:
        return None


def lookup_macro(courier_id, macro_name, session, db_svc):
    Macro = db_svc.Base.classes.user_macros
    try:
        return session.query(Macro).filter(Macro.user_id == courier_id, Macro.name == macro_name).one()
    except NoResultFound:
        return None


def courier_is_on_duty(courier_id, session, db_svc):
    Courier = db_svc.Base.classes.couriers
    try:
        courier = session.query(Courier).filter(Courier.id == courier_id).one()
        if courier.duty_status == 1:
            return True
        elif courier.duty_status == 0:
            return False
        else:
            raise Exception('Unrecognized courier duty_status value %s.' % courier.duty_status)
    except NoResultFound:
        # TODO: maybe raise some more hell if we got an invalid courier ID,
        # but this is fine for now
        return False


def courier_has_bid(courier_id, job_tag, session, db_svc):
    JobBid =db_svc.Base.classes.job_bids
    Courier = db_svc.Base.classes.couriers
    try:
        bid = session.query(JobBid).filter(and_(JobBid.courier_id == courier_id,
                                                 JobBid.job_tag == job_tag,
                                                 JobBid.expired_ts == None,
                                                 JobBid.accepted_ts == None)).one()
        return True                                                 
    except NoResultFound:
        return False


def compile_help_string():
    lines = []

    lines.append('________')

    lines.append('[ LIST commands ]:')
    for key, cmd_spec in SMS_GENERATOR_COMMAND_SPECS.items():
        lines.append('%s : %s' % (key, cmd_spec.definition))
    
    lines.append('________')

    lines.append('[ GENERAL commands ]:')
    for key, cmd_spec in SMS_SYSTEM_COMMAND_SPECS.items():
        lines.append('%s : %s' % (key, cmd_spec.definition))
    
    lines.append('________')

    lines.append('[ PREFIX commands ]:')
    for key, cmd_spec in SMS_PREFIX_COMMAND_SPECS.items():
        lines.append('%s : %s' % (key, cmd_spec.definition))

    lines.append('________')

    return '\n\n'.join(lines)


def handle_on_duty(cmd_object, dlg_context, service_registry, **kwargs):
    db_svc = service_registry.lookup('postgres')
    with db_svc.txn_scope() as session:
        courier = lookup_courier_by_id(dlg_context.courier.id, session, db_svc)
        if courier.duty_status == 1:
            return ' '.join([
                'Hello %s, you are already on the duty roster.' % dlg_context.courier.first_name,
                'The system will automatically notify you when a job is posted.'
            ])
        else:
            courier.duty_status = 1
            session.flush()
            return ' '.join([
                'Hello %s, welcome to the on-call roster.' % dlg_context.courier.first_name, 
                'Reply to advertised job tags with the tag and "acc" to accept a job.',
                'Text "hlp" or "?" at any time to see the command codes.'
            ])


def handle_off_duty(cmd_object, dlg_context, service_registry, **kwargs):
    # TODO remove courier from any open bidding pools
    db_svc = service_registry.lookup('postgres')
    with db_svc.txn_scope() as session:
        courier = lookup_courier_by_id(dlg_context.courier.id, session, db_svc)
        if courier.duty_status == 0:
            return ' '.join([
                'Hello %s, you are already off duty.' % dlg_context.courier.first_name,
                'Enjoy the downtime!'
            ])

        else:
            courier.duty_status = 0
            session.flush()
            return ' '.join([
                'Hello %s, you are now leaving the on-call roster.' % dlg_context.courier.first_name,
                'Thank you for your service. Have a good one!'
            ])


def handle_bid_for_job(cmd_object, dlg_context, service_registry, **kwargs):
    if not cmd_object.job_tag:
        return 'Bid for a job by texting the job tag, space, and "bid".'

    if not is_valid_job_tag(cmd_object.job_tag):
        return REPLY_INVALID_TAG_TPL % cmd_object.job_tag

    db_svc = service_registry.lookup('postgres')
    with db_svc.txn_scope() as session:
        # make sure the job is open
        if not job_is_available(cmd_object.job_tag, session, db_svc):
            return ' '.join(['The job with tag:',
                              cmd_object.job_tag,
                              'is not in the pool of available jobs.',
                              'Text "opn" for a list of open jobs.'
                            ])
        
        if not courier_is_on_duty(dlg_context.courier.id, session, db_svc):
            # automatically place this courier on the duty roster
            payload = {
                'id': dlg_context.courier.id,
                'status': 1 # 1 means on-duty
            }
            transform_status = update_courier_status_func(payload, service_registry, **kwargs)
            if not transform_status.ok:
                print(transform_status)
                return 'There was an error attempting to auto-update your duty status. Please contact your administrator.'
        
        # only one bid per user (TODO: pluggable bidding policy)
        if courier_has_bid(dlg_context.courier.id, cmd_object.job_tag, session, db_svc):
            return ' '.join([
                'You have already bid on the job:',
                cmd_object.job_tag,
                "Once the bid window closes, we'll text you if you get the assignment.",
                "Good luck!"
            ])

        bidding_window = lookup_open_bidding_window_by_job_tag(cmd_object.job_tag, session, db_svc)
        if not bidding_window:
            return ' '.join([
                "Sorry, the bidding window for job:",
                cmd_object.job_tag,
                "has closed."
            ])

        # job exists and is available, so bid for it
        kwargs['job_tag'] = cmd_object.job_tag
        bid = ObjectFactory.create_job_bid(db_svc,
                                           bidding_window_id=bidding_window.id,
                                           courier_id=dlg_context.courier.id,
                                           job_tag=cmd_object.job_tag)
        session.add(bid)

        return ' '.join([
            "Thank you! You've made a bid to accept job:",
            cmd_object.job_tag,
            "If you get the assignment, we'll text you when the bidding window closes."
        ])

    
def handle_accept_job(cmd_object, dlg_context, service_registry, **kwargs):
    # TODO: verify (non-stale) assignment, update status table
    current_time = datetime.datetime.now()
    job_tag = cmd_object.job_tag
    if not job_tag:
        return 'To accept a job assignment, text the job tag, a space, and "acc".'

    db_svc = service_registry.lookup('postgres')
    with db_svc.txn_scope() as session:
        try:
            # first, does this user even own this job?
            job_bid = lookup_user_job_bid(job_tag, dlg_context.courier.id, session, db_svc)

            if not job_bid:
                return 'Sorry, it appears the job with tag %s is either expired or not yours to accept.' % job_tag

            if lookup_open_bidding_window_by_job_tag(job_tag, session, db_svc):
                return 'Sorry -- the bidding window for this job is still open.'

            jobstat = lookup_current_job_status(job_tag, session, db_svc)
            if jobstat.status == JOB_STATUS_ACCEPTED:
                return 'You have already accepted this job.'

            else:
                # expire this job status and create a new one
                jobstat.expired_ts = current_time
                session.add(jobstat)

                new_status = ObjectFactory.create_job_status(db_svc,
                                                             job_tag=job_tag,
                                                             status=JOB_STATUS_ACCEPTED,
                                                             write_ts=current_time)
                session.add(new_status)

                jobdata = lookup_job_data_by_tag(job_tag, session, db_svc)
                if not jobdata:
                    raise Exception('No job data entry found for job tag %s.' % job_tag)

                new_assignment = ObjectFactory.create_job_assignment(db_svc,
                                                                     courier_id=dlg_context.courier.id,
                                                                     job_id=jobdata.id,
                                                                     job_tag=job_tag)
                session.add(new_assignment)
                session.flush()         

                return ' '.join([
                    'You have accepted job %s.' % job_tag,
                    'This job will show up in your personal job queue,',
                    'which you can review by texting "my".',
                    'You can get the details of this job by texting',
                    'The job tag, space, and "dt".',
                    'Godspeed!' 
                ])

        except Exception as err:
            print(err)
            traceback.print_exc(file=sys.stdout)
            session.rollback()
            return 'There was an error while attempting to accept this job. Please contact your administrator.'


def handle_help(cmd_object, dlg_context, service_registry, **kwargs):
    return compile_help_string()


def handle_job_details(cmd_object, dlg_context, service_registry, **kwargs):
    if cmd_object.job_tag == None:
        return 'To receive details on a job, text the job tag, a space, and "dt".'

    db_svc = service_registry.lookup('postgres')
    with db_svc.txn_scope() as session:
        job = lookup_job_data_by_tag(cmd_object.job_tag, session, db_svc)
        if not job:
            return 'The job with tag "%s" is either not in the system, or has already been scheduled.' % cmd_object.job_tag
        
        lines = []
        lines.append('pickup address: %s' % job.pickup_address)
        lines.append('pickup borough: %s' % job.pickup_borough)
        lines.append('pickup neighborhood: %s' % job.pickup_neighborhood)
        lines.append('pickup zipcode: %s' % job.pickup_zip)

        lines.append('delivery address: %s' % job.pickup_address)
        lines.append('delivery borough: %s' % job.delivery_borough)
        lines.append('delivery zipcode: %s' % job.delivery_zip)
        lines.append('items: %s' % job.items)

        return '\n\n'.join(lines)


def handle_en_route(cmd_object, dlg_context, service_registry, **kwargs):
    current_time = datetime.datetime.now()
    db_svc = service_registry.lookup('postgres')
    with db_svc.txn_scope() as session:
        try:
            jobs = list_accepted_jobs(dlg_context.courier.id, session, db_svc)

            if not len(jobs):
                return 'There are no jobs in your queue.'

            if not cmd_object.job_tag:
                if len(jobs) == 1:
                    # if thereis only one job in the queue, that's the one
                    job_tag = jobs[0].job_tag
                else:
                    return ' '.join([
                        "To notify the network that you're en route,",
                        "please text the tag of the job you're starting,",
                        'space, then "%s".' % cmd_object.cmdspec.command
                    ])
            else:
                job_tag = cmd_object.job_tag

            if not job_belongs_to_courier(job_tag, dlg_context.courier.id, session, db_svc):
                return 'Job with tag %s does not appear to be one of yours.' % job_tag

            print('###  performing en-route status update for job %s...' % job_tag)
            current_job_status = lookup_current_job_status(job_tag,
                                                           session,
                                                           db_svc)

            if current_job_status.status == JOB_STATUS_IN_PROGRESS:
                return 'You have already reported en-route status for this job.'
            else:
                current_job_status.expired_ts = current_time
                session.add(current_job_status)
                new_job_status = ObjectFactory.create_job_status(db_svc,
                                                                    job_tag=job_tag,
                                                                    status=JOB_STATUS_IN_PROGRESS,
                                                                    write_ts=current_time)
                session.add(new_job_status)
                session.flush()

                return ' '.join([
                    "You have reported that you're en route for job:",
                    '%s. Godspeed!' % job_tag
                ])
        
        except Exception as err:
            session.rollback()
            print(err)
            traceback.print_exc(file=sys.stdout)
            return 'There was an error updating the status of this job. Please contact your administrator.'

    

def handle_cancel_job(cmd_object, dlg_context, service_registry, **kwargs):
    # TODO: handle missing job tag
    # TODO: update status table
    if not cmd_object.job_tag:
        return 'To cancel a job, text the job tag, a space, and "can".'
    
    # TODO: add cancel logic
    return "Recording job cancellation for job tag: %s" % cmd_object.job_tag
    

def handle_job_finished(cmd_object, dlg_context, service_registry, **kwargs):
    current_time = datetime.datetime.now()
    db_svc = service_registry.lookup('postgres')
    with db_svc.txn_scope() as session:
        try:
            job_tag = None
            jobs = list_accepted_jobs(dlg_context.courier.id, session, db_svc)
            if not len(jobs):
                return 'There are no jobs in your queue.'
            
            if not cmd_object.job_tag:
                if len(jobs) == 1:
                    # only one job in your queue; this must be it
                    job_tag = jobs[0].job_tag

                elif len(jobs) > 1:
                    return ' '.join([
                        'To notify the system that you have completed a job,',
                        'text the job tag, space, and "%s".' % cmd_object.cmdspec.command
                    ])
            else:
                job_tag = cmd_object.job_tag

            if not job_belongs_to_courier(job_tag, dlg_context.courier.id, session, db_svc):
                return 'Job with tag %s does not appear to be one of yours.' % job_tag

            print('###  performing ->complete status update for job %s...' % job_tag)
            current_job_status = lookup_current_job_status(job_tag,
                                                           session,
                                                           db_svc)

            if current_job_status.status == JOB_STATUS_COMPLETED:
                return 'You have already reported that this job is complete. Thanks again!'
            else:
                current_job_status.expired_ts = current_time
                session.add(current_job_status)
                new_job_status = ObjectFactory.create_job_status(db_svc,
                                                                 job_tag=job_tag,
                                                                 status=JOB_STATUS_COMPLETED,
                                                                 write_ts=current_time)
                session.add(new_job_status)
                session.flush()

                return ' '.join([
                    'Recording job completion for job tag:',
                    '%s. Thank you!' % job_tag
                ])

        except Exception as err:
            session.rollback()
            print(err)
            traceback.print_exc(file=sys.stdout)
            return 'There was an error updating the status of this job. Please contact your administrator.'


def handle_emergency(cmd_object, dlg_context, service_registry, **kwargs):
    # TODO: add broadcast logic
    courier_name = '%s %s' % (dlg_context.courier.first_name, dlg_context.courier.last_name)
    return ' '.join([
        "Reporting an emergency for courier %s" % courier_name,
        "with mobile phone number %s." % dlg_context.source_number, 
        "To report a crime or a life-threatening emergency, ",
        "please IMMEDIATELY call 911." 
    ])
    

def handle_bidding_status_for_job(cmd_object, dlg_context, service_registry, **kwargs):
    # TODO: return actual bidding status (is bidding open? closed? Has job been awarded? Accepted?)
    if not cmd_object.job_tag:
        return 'To see the bidding status of a job, text the job tag, space, and "bst".'

    return "Placeholder for reporting bidding status of a job"


def extension_is_positive_num(ext_string):
    if INTEGER_RX.match(ext_string):
        return True
    return False

def extension_is_negative_num(ext_string):
    if NEG_INTEGER_RX.match(ext_string):
        return True
    return False

def extension_is_range(ext_string):
    if RANGE_RX.match(ext_string):
        return True
    return False


def generate_list_my_awarded_jobs(cmd_object, dlg_engine, dlg_context, service_registry, **kwargs):
    '''List jobs which have been awarded to this courier in the bidding process
    (but not yet accepted).
    '''

    db_svc = service_registry.lookup('postgres')
    with db_svc.txn_scope() as session:
        jobs = list_awarded_jobs(dlg_context.courier.id, session, db_svc)
        if not len(jobs):
            return 'Either you have accepted all your awarded jobs, or you have not been awarded any.'

        tokens = cmd_object.cmd_string.split(cmd_object.cmdspec.specifier)
        if len(tokens) == 1:
            # show all job tags in the list

            lines = []
            index = 1
            for job in jobs:
                lines.append("# %d: %s" % (index, job.job_tag))
                index += 1
            
            return '\n\n'.join(lines)
        else:
            ext = tokens[1] 
            # the "extension" is the part of the command string immediately following the specifier character
            # (we have defaulted to a period, but that's settable).
            #
            # if we receive <cmd><specifier>N where N is an integer, return the Nth item in the list
            if extension_is_positive_num(ext):
                list_index=int(ext)

                if list_index > len(jobs):
                    return "You requested job # %d, but you have only been awarded %d jobs." % (list_index, len(jobs))
                if list_index == 0:
                    return "You may not request the 0th element of a list. (Nice try, C programmers.)"

                list_element = jobs[list_index-1].job_tag
                
                # if the user is extracting a single list element (by using an integer extension), we do 
                # one of two things. If there were no command modifiers specified, we simply return the element:

                if not len(cmd_object.modifiers):
                    return list_element
                else:
                    # ...but if there were modifiers, then we construct a new command by chaining the output of this command 
                    # with the modifier array.
                    command_tokens = [list_element]
                    command_tokens.extend(cmd_object.modifiers)

                    # TODO: instead of splitting on this char, urldecode the damn thing from the beginning
                    command_string = '+'.join(command_tokens)
                    chained_command = parse_sms_message_body(command_string)

                    print('command: ' + str(chained_command))                                       
                    return dlg_engine.reply_command(chained_command, dlg_context, service_registry)

            elif extension_is_negative_num(ext):
                neg_index = int(ext)

                if neg_index == 0:
                    return '-0 is not a valid negative index. Use -1 to specify the last job in the list.' 

                zero_list_index = len(jobs) + neg_index

                if zero_list_index < 0:
                    return 'You specified a negative list offset (%d), but you only have %d awarded jobs.' % (neg_index, len(jobs))
                list_element = jobs[zero_list_index].job_tag

                if not len(cmd_object.modifiers):
                    return list_element
                else:
                    # ...but if there were modifiers, then we construct a new command by chaining the output of this command 
                    # with the modifier array.
                    command_tokens = [list_element]
                    command_tokens.extend(cmd_object.modifiers)

                    # TODO: instead of splitting on this char, urldecode the damn thing from the beginning
                    command_string = '+'.join(command_tokens)
                    chained_command = parse_sms_message_body(command_string)

                    print('command: ' + str(chained_command))                                       
                    return dlg_engine.reply_command(chained_command, dlg_context, service_registry)

            elif extension_is_range(ext):
                # if we receive <cmd><specifier>N-M where N and M are both integers, return the Nth through the Mth items
                tokens = ext.split('-')
                if len(tokens) != 2:
                    return 'The range extension for this command must be formatted as A-B where A and B are integers.'

                min_index = int(tokens[0])
                max_index = int(tokens[1])
                
                if min_index > max_index:
                    return 'The first number in your range specification A-B must be less than or equal to the second number.'

                if max_index > len(jobs):
                    return "There are only %d jobs open." % len(jobs)

                if min_index == 0:
                    return "You may not request the 0th element of a list. (This stack was written in Python, but the UI is in English.)"

                lines = []
                for index in range(min_index, max_index+1):
                    lines.append('# %d: %s' % (index, jobs[index-1].job_tag))
                
                return '\n\n'.join(lines)


def generate_list_my_accepted_jobs(cmd_object, dlg_engine, dlg_context, service_registry, **kwargs):
    jobs = []
    db_svc = service_registry.lookup('postgres')
    with db_svc.txn_scope() as session:
        
        JobAssignment = db_svc.Base.classes.job_assignments
        JobStatus = db_svc.Base.classes.job_status
        # resultset = session.query(JobAssignment).filter(JobAssignment.courier_id == dlg_context.courier.id).all()

        for ja, stat in session.query(JobAssignment,
                                      JobStatus).filter(and_(JobStatus.job_tag == JobAssignment.job_tag,
                                                             JobStatus.status == JOB_STATUS_ACCEPTED,
                                                             JobStatus.expired_ts == None,
                                                             JobAssignment.courier_id == dlg_context.courier.id)).all():
            jobs.append(stat)

        if not len(jobs):
            return 'You have no current accepted jobs in your queue.'

        tokens = cmd_object.cmd_string.split(cmd_object.cmdspec.specifier)
        if len(tokens) == 1:
            lines = []
            index = 1
            # if no specifier is present in the command string, return the entire list (with indices)
            #TODO: segment output for very long lists
            for job in jobs:
                lines.append("# %d: %s" % (index, job.job_tag))
                index += 1
            
            return '\n\n'.join(lines)
            
        else:
            ext = tokens[1] 
            # the "extension" is the part of the command string immediately following the specifier character
            # (we have defaulted to a period, but that's settable).
            #
            # if we receive <cmd><specifier>N where N is an integer, return the Nth item in the list
            if extension_is_positive_num(ext):
                list_index=int(ext)

                if list_index > len(jobs):
                    return "You requested open job # %d, but there are only %d jobs open." % (list_index, len(jobs))
                if list_index == 0:
                    return "You may not request the 0th element of a list. (Nice try, C programmers.)"

                list_element = jobs[list_index-1].job_tag
                
                # if the user is extracting a single list element (by using an integer extension), we do 
                # one of two things. If there were no command modifiers specified, we simply return the element:

                if not len(cmd_object.modifiers):
                    return list_element
                else:
                    # ...but if there were modifiers, then we construct a new command by chaining the output of this command 
                    # with the modifier array.
                    command_tokens = [list_element]
                    command_tokens.extend(cmd_object.modifiers)

                    # TODO: instead of splitting on this char, urldecode the damn thing from the beginning
                    command_string = '+'.join(command_tokens)
                    chained_command = parse_sms_message_body(command_string)

                    print('command: ' + str(chained_command))                                       
                    return dlg_engine.reply_command(chained_command, dlg_context, service_registry)

            elif extension_is_negative_num(ext):
                neg_index = int(ext)

                if neg_index == 0:
                    return '-0 is not a valid negative index. Use -1 to specify the last job in the list.' 

                zero_list_index = len(jobs) + neg_index

                if zero_list_index < 0:
                    return 'You specified a negative list offset (%d), but there are only %d open jobs.' % (neg_index, len(jobs))
                list_element = jobs[zero_list_index].job_tag

                if not len(cmd_object.modifiers):
                    return list_element
                else:
                    # ...but if there were modifiers, then we construct a new command by chaining the output of this command 
                    # with the modifier array.
                    command_tokens = [list_element]
                    command_tokens.extend(cmd_object.modifiers)

                    # TODO: instead of splitting on this char, urldecode the damn thing from the beginning
                    command_string = '+'.join(command_tokens)
                    chained_command = parse_sms_message_body(command_string)

                    print('command: ' + str(chained_command))                                       
                    return dlg_engine.reply_command(chained_command, dlg_context, service_registry)

            elif extension_is_range(ext):
                # if we receive <cmd><specifier>N-M where N and M are both integers, return the Nth through the Mth items
                tokens = ext.split('-')
                if len(tokens) != 2:
                    return 'The range extension for this command must be formatted as A-B where A and B are integers.'

                min_index = int(tokens[0])
                max_index = int(tokens[1])
                
                if min_index > max_index:
                    return 'The first number in your range specification A-B must be less than or equal to the second number.'

                if max_index > len(jobs):
                    return "There are only %d jobs open." % len(jobs)

                if min_index == 0:
                    return "You may not request the 0th element of a list. (This stack was written in Python, but the UI is in English.)"

                lines = []
                for index in range(min_index, max_index+1):
                    lines.append('# %d: %s' % (index, jobs[index-1].job_tag))
                
                return '\n\n'.join(lines)
            


def generate_list_open_jobs(cmd_object, dlg_engine, dlg_context, service_registry, **kwargs):
    print('#--- Generating open job listing...')
    db_svc = service_registry.lookup('postgres')
    with db_svc.txn_scope() as session:
        jobs = list_available_jobs(session, db_svc)
        if not len(jobs):
            return 'No open jobs found.'

        tokens = cmd_object.cmd_string.split(cmd_object.cmdspec.specifier)
        if len(tokens) == 1:
            lines = []
            index = 1

            # if no specifier is present in the command string, return the entire list (with indices)
            #TODO: segment output for very long lists
            for job in jobs:
                lines.append("# %d: %s" % (index, job.job_tag))
                index += 1
            
            return '\n\n'.join(lines)
        else:
            ext = tokens[1] 
            # the "extension" is the part of the command string immediately following the specifier character
            # (we have defaulted to a period, but that's settable).
            #
            # if we receive <cmd><specifier>N where N is an integer, return the Nth item in the list
            if extension_is_positive_num(ext):
                list_index=int(ext)

                if list_index > len(jobs):
                    return "You requested open job # %d, but there are only %d jobs open." % (list_index, len(jobs))
                if list_index == 0:
                    return "You may not request the 0th element of a list. (Nice try, C programmers.)"

                list_element = jobs[list_index-1].job_tag
                
                # if the user is extracting a single list element (by using an integer extension), we do 
                # one of two things. If there were no command modifiers specified, we simply return the element:

                if not len(cmd_object.modifiers):
                    return list_element
                else:
                    # ...but if there were modifiers, then we construct a new command by chaining the output of this command 
                    # with the modifier array.
                    command_tokens = [list_element]
                    command_tokens.extend(cmd_object.modifiers)

                    # TODO: instead of splitting on this char, urldecode the damn thing from the beginning
                    command_string = '+'.join(command_tokens)
                    chained_command = parse_sms_message_body(command_string)

                    print('command: ' + str(chained_command))                                       
                    return dlg_engine.reply_command(chained_command, dlg_context, service_registry)

            elif extension_is_negative_num(ext):
                neg_index = int(ext)

                if neg_index == 0:
                    return '-0 is not a valid negative index. Use -1 to specify the last job in the list.' 

                zero_list_index = len(jobs) + neg_index

                if zero_list_index < 0:
                    return 'You specified a negative list offset (%d), but there are only %d open jobs.' % (neg_index, len(jobs))
                list_element = jobs[zero_list_index].job_tag

                if not len(cmd_object.modifiers):
                    return list_element
                else:
                    # ...but if there were modifiers, then we construct a new command by chaining the output of this command 
                    # with the modifier array.
                    command_tokens = [list_element]
                    command_tokens.extend(cmd_object.modifiers)

                    # TODO: instead of splitting on this char, urldecode the damn thing from the beginning
                    command_string = '+'.join(command_tokens)
                    chained_command = parse_sms_message_body(command_string)

                    print('command: ' + str(chained_command))                                       
                    return dlg_engine.reply_command(chained_command, dlg_context, service_registry)

            elif extension_is_range(ext):
                # if we receive <cmd><specifier>N-M where N and M are both integers, return the Nth through the Mth items
                tokens = ext.split('-')
                if len(tokens) != 2:
                    return 'The range extension for this command must be formatted as A-B where A and B are integers.'

                min_index = int(tokens[0])
                max_index = int(tokens[1])
                
                if min_index > max_index:
                    return 'The first number in your range specification A-B must be less than or equal to the second number.'

                if max_index > len(jobs):
                    return "There are only %d jobs open." % len(jobs)

                if min_index == 0:
                    return "You may not request the 0th element of a list. (This stack was written in Python, but the UI is in English.)"

                lines = []
                for index in range(min_index, max_index+1):
                    lines.append('# %d: %s' % (index, jobs[index-1].job_tag))
                
                return '\n\n'.join(lines)


def pfx_command_sethandle(prefix_cmd, dlg_engine, dlg_context, service_registry):
    handle = prefix_cmd.name
    db_svc = service_registry.lookup('postgres')
    with db_svc.txn_scope() as session:
        try:
            handle_entry = lookup_live_courier_handle(dlg_context.courier.id, session, db_svc)
            if handle_entry:
                if handle_entry.handle == handle:
                    return 
                else:
                    handle_entry.expired_ts = datetime.datetime.now()
                    session.add(handle_entry)
                    session.flush()

            # ignore the mode; the command name is the user handle
            payload = {
                'user_id': dlg_context.courier.id,
                'handle': handle,
                'is_public': True,
                'created_ts': datetime.datetime.now()
            }

            new_handle_entry = ObjectFactory.create_user_handle(db_svc, **payload)
            session.add(new_handle_entry)
            session.flush()

            return ' '.join([
                'Your user handle has been set to %s.' % new_handle_entry.handle,
                'A system user can send a message to your log by texting:',
                '@%s, space, and the message.' % handle
            ])

        except Exception as err:
            print('exception of type %s thrown: %s' % (err.__class__.__name__, str(err)))
            session.rollback()
            return 'There was an error creating your user handle. Please contact your administrator.'
            

def pfx_command_sendlog(prefix_cmd, dlg_engine, dlg_context, service_registry):
    db_svc = service_registry.lookup('postgres')
    with db_svc.txn_scope() as session:
        try:
            if prefix_cmd.mode == 'simple':
                return ' '.join([
                    "To send a message to a user's log,",
                    'text @<user handle>, space, and the message.'
                ])

            elif prefix_cmd.mode == 'extended':
                target_handle = prefix_cmd.name
                message = prefix_cmd.body
                to_courier = lookup_courier_by_handle(target_handle, session, db_svc)

                if not to_courier:
                    return 'Courier with handle %s not found.' % target_handle
                
                payload = {
                    'from_user': dlg_context.courier.id,
                    'to_user': to_courier.id,
                    'msg_type': 1, 
                    'mime_type': 'text/plain',
                    'msg_data': message
                }

                log_record = ObjectFactory.create_user_log(db_svc, **payload)
                session.add(log_record)
                session.flush()

                return 'Message sent.'

        except Exception as err:
            print('exception of type %s thrown: %s' % (err.__class__.__name__, str(err)))
            session.rollback()
            return 'There was an error sending your message. Please contact your administrator.'


def pfx_command_macro(prefix_cmd, dlg_engine, dlg_context, service_registry):
    # mode is either 'define' or 'execute'
    macro = None
    db_svc = service_registry.lookup('postgres')

    with db_svc.txn_scope() as session:
        # in extended mode, a prefix command contains a name and a body,
        # separated by the "defchar" found in the prefix's command spec
        #
        if prefix_cmd.mode == 'extended': 
            # TODO: filter out invalid command body strings / check for max length
            payload = {
                'user_id': dlg_context.courier.id,
                'name': prefix_cmd.name,
                'command_string': prefix_cmd.body
            }

            macro = ObjectFactory.create_macro(db_svc, **payload)
            session.add(macro)
            session.flush()

        # in simple mode, a prefix command contains only the command name
        elif prefix_cmd.mode == 'simple':
            macro = lookup_macro(dlg_context.courier.id,
                                 prefix_cmd.name,
                                 session,
                                 db_svc)
            if not macro:
                return 'No macro %s%s has been registered under your user ID.' % (prefix_cmd.cmdspec.command, prefix_cmd.name)

            chained_command = parse_sms_message_body(macro.command_string)
            return dlg_engine.reply_command(chained_command, dlg_context, service_registry)            

    print('Courier %s created macro: macro' % macro)
    return 'Command macro %s%s registered.' % (prefix_cmd.cmdspec.command, prefix_cmd.name)
        

SMSDialogContext = namedtuple('SMSDialogContext', 'courier source_number message')

class DialogEngine(object):
    def __init__(self):
        self.msg_dispatch_tbl = {}
        self.generator_dispatch_tbl = {}
        self.prefix_dispatch_tbl = {}


    def register_cmd_spec(self, sms_command_spec, handler_func):
        self.msg_dispatch_tbl[str(sms_command_spec)] = handler_func

    def register_generator_cmd(self, generator_cmd_spec, handler_func):
        self.generator_dispatch_tbl[str(generator_cmd_spec)] = handler_func

    def register_prefix_cmd(self, prefix_spec, handler_func):
        self.prefix_dispatch_tbl[str(prefix_spec)] = handler_func


    def _reply_prefix_command(self, prefix_cmd, dialog_context, service_registry, **kwargs):
        command = self.prefix_dispatch_tbl.get(str(prefix_cmd.cmdspec))
        if not command:
            return 'No handler registered in SMS DialogEngine for prefix command %s.' % prefix_cmd.cmdspec.command
        return command(prefix_cmd, self, dialog_context, service_registry)


    def _reply_generator_command(self, gen_cmd, dialog_context, service_registry, **kwargs):
        list_generator = self.generator_dispatch_tbl.get(str(gen_cmd.cmdspec))
        if not list_generator:
            return 'No handler registered in SMS DialogEngine for generator command %s.' % gen_cmd.cmdspec.command
        return list_generator(gen_cmd, self, dialog_context, service_registry)


    def _reply_sys_command(self, sys_cmd, dialog_context, service_registry, **kwargs):
        handler = self.msg_dispatch_tbl.get(str(sys_cmd.cmdspec))
        if not handler:
            return 'No handler registered in SMS DialogEngine for system command %s.' % sys_cmd.cmdspec.command
        return handler(sys_cmd, dialog_context, service_registry)


    def reply_command(self, command_input, dialog_context, service_registry, **kwargs):
        # command types: generator, syscommand, prefix
        if command_input.cmd_type == 'prefix':
            return self._reply_prefix_command(command_input.cmd_object, dialog_context, service_registry)

        elif command_input.cmd_type == 'syscommand':
            return self._reply_sys_command(command_input.cmd_object, dialog_context, service_registry)

        elif command_input.cmd_type == 'generator':
            return self._reply_generator_command(command_input.cmd_object, dialog_context, service_registry)

        else:
            raise Exception('Unrecognized command input type %s.' % command_input.cmd_type)


def sms_responder_func(input_data, service_objects, **kwargs):
    db_svc = service_objects.lookup('postgres')
    sms_svc = service_objects.lookup('sms')

    engine = DialogEngine()
    
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['bid'], handle_bid_for_job)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['bst'], handle_bidding_status_for_job)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['acc'], handle_accept_job)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['dt'], handle_job_details)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['ert'], handle_en_route)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['can'], handle_cancel_job)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['fin'], handle_job_finished)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['911'], handle_emergency)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['hlp'], handle_help)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['on'], handle_on_duty)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['off'], handle_off_duty)

    engine.register_generator_cmd(SMS_GENERATOR_COMMAND_SPECS['my'], generate_list_my_accepted_jobs)
    engine.register_generator_cmd(SMS_GENERATOR_COMMAND_SPECS['awd'], generate_list_my_awarded_jobs)
    engine.register_generator_cmd(SMS_GENERATOR_COMMAND_SPECS['opn'], generate_list_open_jobs)

    engine.register_prefix_cmd(SMS_PREFIX_COMMAND_SPECS['$'], pfx_command_macro)
    engine.register_prefix_cmd(SMS_PREFIX_COMMAND_SPECS['@'], pfx_command_sendlog)
    engine.register_prefix_cmd(SMS_PREFIX_COMMAND_SPECS['&'], pfx_command_sethandle)

    print('###------ SMS payload:')
    source_number = input_data['From']
    raw_message_body = input_data['Body']
    
    print('###')
    print('###------ Received raw message "%s" from mobile number [%s].' % (raw_message_body, source_number))
    print('###')

    mobile_number = normalize_mobile_number(source_number)

    courier = None
    with db_svc.txn_scope() as session:
        courier = lookup_courier_by_mobile_number(mobile_number, session, db_svc)
        if not courier:
            print('Courier with mobile number %s not found.' % mobile_number)
            sms_svc.send_sms(mobile_number, REPLY_NOT_IN_NETWORK)
            return core.TransformStatus(ok_status('SMS event received', is_valid_command=False))
        
        session.expunge(courier)        
        
    dlg_context = SMSDialogContext(courier=courier, source_number=mobile_number, message=unquote_plus(raw_message_body))

    try:
        command_input = parse_sms_message_body(raw_message_body)
        print('#----- Resolved command: %s' % str(command_input))

        response = engine.reply_command(command_input, dlg_context, service_objects)
        sms_svc.send_sms(mobile_number, response)

        return core.TransformStatus(ok_status('SMS event received', is_valid_command=True, command=command_input))

    except IncompletePrefixCommand as err:
        print('Error data: %s' % err)
        print('#----- Incomplete prefix command: in message body: %s' % raw_message_body)
        sms_svc.send_sms(mobile_number, SMS_PREFIX_COMMAND_SPECS[raw_message_body].definition)
        return core.TransformStatus(ok_status('SMS event received', is_valid_command=False))

    except UnrecognizedSMSCommand as err:
        print('Error data: %s' % err)
        print('#----- Unrecognized system command: in message body: %s' % raw_message_body)
        sms_svc.send_sms(mobile_number, compile_help_string())
        return core.TransformStatus(ok_status('SMS event received', is_valid_command=False))
    

def poll_job_status_func(input_data, service_objects, **kwargs):
    db_svc = service_objects.lookup('postgres')
    JobStatus = db_svc.Base.classes.job_status
    tag = input_data['job_tag']

    status = None
    with db_svc.txn_scope() as session:
        result = session.query(JobStatus).filter(and_(JobStatus.job_tag == tag, JobStatus.expired_ts == None)).one()
        status = result.status

    return core.TransformStatus(ok_status('poll request', job_tag=tag, job_status=status))


def update_job_log_func(input_data, service_objects, **kwargs):
    raise snap.TransformNotImplementedException('update_job_log_func')


def couriers_by_status_func(input_data, service_objects, **kwargs):
    status = input_data['status']
    courier_records = None
    db_svc = service_objects.lookup('postgres')
    with db_svc.txn_scope() as session:
        courier_records = lookup_couriers_by_status(status, session, db_svc)
    
    return core.TransformStatus(ok_status('couriers by status', courier_status=status, couriers=courier_records))
    

def update_courier_status_func(input_data, service_objects, **kwargs):
    new_status = input_data['status']
    courier_id = input_data['id']

    db_svc = service_objects.lookup('postgres')
    with db_svc.txn_scope() as session:
        courier = lookup_courier_by_id(courier_id, session, db_svc)

        if courier.duty_status == new_status:
            did_update = False
        else:
            courier.duty_status = new_status
            session.add(courier)
            did_update = True

    return core.TransformStatus(ok_status('update courier status', updated=did_update, id=courier_id, duty_status=new_status))


def open_bidding_func(input_data, service_objects, **kwargs):
    '''Open a bidding window on the job with the job_tag passed in the inputs
    '''

    bidding_window = None
    db_svc = service_objects.lookup('postgres')
    with db_svc.txn_scope() as session:
        input_record = prepare_bid_window_record(input_data, session, db_svc)
        bidding_window = ObjectFactory.create_bidding_window(db_svc, **input_record)
        session.add(bidding_window)
        session.flush()

    return core.TransformStatus(ok_status('open bidding window',
                                          id=bidding_window.id,
                                          job_id=bidding_window.job_id,
                                          job_tag=bidding_window.job_tag))


def close_bidding_func(input_data, service_objects, **kwargs):
    '''Close the bidding window with the ID passed in the inputs
    '''

    bidding_window = None
    db_svc = service_objects.lookup('postgres')
    with db_svc.txn_scope() as session:
        bidding_window = lookup_bidding_window_by_id(input_data['id'], session, db_svc)
        if not bidding_window:
            return core.TransformStatus(ok_status('close bidding window',
                                                  id=input_data['id'],
                                                  message='window not found'))

        window.close_ts = datetime.datetime.now()

    return core.TransformStatus(ok_status('close bidding window',
                                          id=input_data['id']))                                             



def active_job_bids_func(input_data, service_objects, **kwargs):
    '''Return a list of bids on the job passed in the input data,
    provided the job is active (calling this function against a job whose bidding window 
    has closed, should return an empty list).

    TODO: think about representing bid windows with a bid_windows table. In the current implementation,
    each bidder only bids once -- but there are use cases in which one might accept multiple bids
    from a single participant (for example, an actual auction). In such a case, *the expiration of 
    a bid* is a completely separate matter from *the bidding window itself having closed* (that is, 
    the auction concluding for a particular item), because each bid from a given participant auto-expires
    his previous bid -- but the bidding window is obviously still open. --DT
    '''

    job_tag = input_data['job_tag']
    bid_list = []
    db_svc = service_objects.lookup('postgres')
    JobBid = db_svc.Base.classes.job_bids
    Courier = db_svc.Base.classes.couriers

    with db_svc.txn_scope() as session:
        try:
            for c, jb in session.query(Courier, JobBid).filter(and_(Courier.id == JobBid.courier_id,
                                                                    JobBid.job_tag == job_tag,
                                                                    JobBid.accepted_ts == None,
                                                                    JobBid.expired_ts == None)).all(): 

                bid_list.append({
                    'bid_id': jb.id,
                    'job_tag': jb.job_tag,
                    'courier_id': c.id,
                    'first_name': c.first_name,
                    'last_name': c.last_name,
                    'mobile_number': c.mobile_number
                })
            return core.TransformStatus(ok_status('get active job bidders', bidders=bid_list))
        except Exception as err:
            return core.TransformStatus(exception_status(err), False, message=str(err))


def award_job_func(input_data, service_objects, **kwargs):
    '''
    print(common.jsonpretty(input_data))
    return core.TransformStatus(ok_status('debugging', data=input_data))
    '''
    print('###-------- awarding job to winning bids')
    print('----------------INPUT DATA\n\n')
    print(input_data)
    print('\n\n----------------INPUT DATA ENDS')

    award_message_lines = [
        "Hello {name}, you've been awarded the job with tag:",
        "{job_tag}.",
        "To accept this job, text the job tag, space, and {accept_command}."
    ]

    award_message_template = ' '.join(award_message_lines)

    window_id = input_data['window_id']
    current_time = datetime.datetime.now()
    notify_targets = {}

    sms_svc = service_objects.lookup('sms')
    db_svc = service_objects.lookup('postgres')

    with db_svc.txn_scope() as session:
        try:
            print('### closing the bidding window %s...' % window_id)
            # close the bidding window
            window = lookup_bidding_window_by_id(window_id, session, db_svc)
            if not window:
                raise Exception('Bidding window with ID %s not found.' % window_id)

            window.close_ts = current_time
            session.add(window)

            # record the winning bids as having been accepted
            
            for bid_record in input_data['bids']:
                
                print('### updating bid to accepted...')
                bid = lookup_bid_by_id(bid_record['bid_id'], session, db_svc)
                bid.accepted_ts = current_time
                session.add(bid)

                notify_targets[bid_record['mobile_number']] = {
                    'name': bid_record['first_name'],
                    'job_tag': bid_record['job_tag'],
                    'accept_command': 'acc'
                }

                # update the status of the job to "awarded"
                
                job_status = lookup_current_job_status(bid_record['job_tag'], session, db_svc)
                if job_status.status == 0:  # 0 is "broadcast"
                    print('### updating the status for job tag %s...' % bid_record['job_tag'])

                    # expire the existing status
                    job_status.expired_ts = current_time
                    session.add(job_status)

                    # add a new status record
                    # status 1 is "awarded"
                    new_job_status = ObjectFactory.create_job_status(db_svc,
                                                                     job_tag=bid_record['job_tag'],
                                                                     status=1,
                                                                     write_ts=current_time)
                    session.add(new_job_status)

            session.flush()

            print('### Sending SMS notification to bid winner(s)...')
            for mobile_number, data in notify_targets.items():
                sms_svc.send_sms(mobile_number, award_message_template.format(**data)) 

            return core.TransformStatus(ok_status('award job to winning bidders',
                                                winners=input_data['bids']))
        except Exception as err:
            session.rollback()
            print(err)
            traceback.print_exc(file=sys.stdout)
            return core.TransformStatus(exception_status(err),
                                        False, 
                                        message='error of type %s closing bid window %s' % (err.__class__.__name__, window_id))


def rebroadcast_func(input_data, service_objects, **kwargs):
    raise snap.TransformNotImplementedException('rebroadcast_func')


def rollover_func(input_data, service_objects, **kwargs):
    raise snap.TransformNotImplementedException('rollover_func')



def bidding_policy_func(input_data, service_objects, **kwargs):
    raise snap.TransformNotImplementedException('bidding_policy_func')


def bidding_status_func(input_data, service_objects, **kwargs):

    windows = []
    db_svc = service_objects.lookup('postgres')
    with db_svc.txn_scope() as session:
        for bwindow in lookup_open_bidding_windows(session, db_svc):
            windows.append({
                'bidding_window_id': bwindow.id,
                'job_id': bwindow.job_id,
                'job_tag': bwindow.job_tag,
                'policy': bwindow.policy,
                'open_ts': bwindow.open_ts.isoformat()
            })

    return core.TransformStatus(ok_status('bidstat', bidding_windows=windows))





