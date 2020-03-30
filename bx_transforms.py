#!/usr/bin/env python

import re
import uuid
import json
import datetime
from collections import namedtuple
from snap import snap, common
from snap import core
from snap.loggers import transform_logger as log
from sqlalchemy.sql import text
import git
import constants as const
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

SYSTEM_ID = 'bxlog'
NETWORK_ID = 'ccr'

INTEGER_RX = re.compile(r'^[0-9]+$')
RANGE_RX = re.compile(r'^[0-9]+\-[0-9]+$')

SMS_SYSTEM_COMMAND_SPECS = {
    'bid': SMSCommandSpec(command='bid', definition='Bid to accept a job', synonyms=[], tag_required=True),
    'bst': SMSCommandSpec(command='bst', definition='Look up the bidding status of this job', synonyms=[], tag_required=True),
    'acc': SMSCommandSpec(command='acc', definition='Accept a delivery job', synonyms=['ac'], tag_required=True),
    'dt': SMSCommandSpec(command='dt', definition='Detail (find out what a particular job entails)', synonyms=[], tag_required=True),
    'er': SMSCommandSpec(command='er', definition='En route to either pick up or deliver for a job', synonyms=[], tag_required=True),
    'can': SMSCommandSpec(command='can', definition='Cancel (courier can no longer complete an accepted job)', synonyms=[], tag_required=True),
    'fin': SMSCommandSpec(command='fin', definition='Finished a delivery', synonyms=['f'], tag_required=True),
    '911': SMSCommandSpec(command='911', definition='Courier is having a problem and needs assistance', synonyms=[], tag_required=False),
    'hlp': SMSCommandSpec(command='hlp', definition='Display help prompts', synonyms=['?'], tag_required=False),
    'on': SMSCommandSpec(command='on', definition='Courier coming on duty', synonyms=[], tag_required=False),
    'off': SMSCommandSpec(command='off', definition='Courier going off duty', synonyms=[], tag_required=False)
}

SMS_GENERATOR_COMMAND_SPECS = {
    'my': SMSGeneratorSpec(command='my', definition='List my pending jobs', specifier='.'), # special command (dot notation),
    'opn': SMSGeneratorSpec(command='opn', definition='List open (available) jobs', specifier='.')
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
REPLY_INVALID_TAG = 'The job tag you have specified appears to be invalid.'

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


def job_is_available(job_tag, session, db_svc):
    JobStatus = db_svc.Base.classes.job_status
    try:
        # status 0 is "broadcast" (available for bidding); status 1 is "accepted-partial" (more than one assignee for the job)
        status_record = session.query(JobStatus).filter(and_(JobStatus.expired_ts == None,
                                                             JobStatus.job_tag == job_tag, 
                                                            or_(JobStatus.status == 0, JobStatus.status == 1))).one()
        return True
    except NoResultFound:
        return False                                                            


def list_available_jobs(session, db_svc):
    jobs = []
    JobStatus = db_svc.Base.classes.job_status
    resultset = session.query(JobStatus).filter(and_(JobStatus.expired_ts == None,
                                                     or_(JobStatus.status == 0, JobStatus.status == 1))).all()
    for record in resultset:
        jobs.append(record)

    return jobs


def prepare_courier_record(input_data, session, db_svc):
    output_record = copy_fields_from(input_data, 'first_name', 'last_name', 'email')
    output_record['mobile_number'] = normalize_mobile_number(input_data['mobile_number'])
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
    couriers = None
    with db_svc.txn_scope() as session:
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
        couriers = lookup_couriers_by_status(1, session, db_svc)

    raw_record['id'] = job_id

    pipeline_svc = service_objects.lookup('job_pipeline')
    s3_svc = service_objects.lookup('s3')
    
    pipeline_svc.post_job_notice(raw_record['job_tag'],
                                 s3_svc, 
                                 job_data=raw_record,
                                 available_couriers=couriers)

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


SystemCommand = namedtuple('SystemCommand', 'job_tag cmdspec modifiers')
GeneratorCommand = namedtuple('GeneratorCommand', 'cmd_string cmdspec modifiers')
HashtagCommand = namedtuple('HashtagCommand', 'tag')

CommandInput = namedtuple('CommandInput', 'cmd_type cmd_object') # command types: generator, syscommand, hashtag


def parse_sms_message_body(raw_body):
    job_tag = None
    command_string = None
    modifiers = []
    
    # make sure there's no leading whitespace, then see what we've got
    body = raw_body.lstrip('+')

    if body.startswith('bxlog-'):
        # remove the URL encoded whitespace chars;
        # remove any trailing/leading space chars as well
        tokens = [token.lstrip('+').rstrip('+') for token in body.split('+') if token]

        print('message tokens: %s' % common.jsonpretty(tokens))

        job_tag = tokens[0]
        if len(tokens) == 2:
            command_string = tokens[1].lower()

        if len(tokens) > 2:
            command_string = tokens[1].lower()
            modifiers = tokens[2:]
    
    elif body.startswith('#'):
        return CommandInput(cmd_type='hashtag', cmd_object=Hashtag('#%s' % body[1:].lower()))

    else:
        tokens = [token.lstrip('+').rstrip('+') for token in body.split('+') if token]
        command_string = tokens[0].lower()
        modifiers = tokens[1:]

        if lookup_generator_command(command_string):
            print('###------------ detected GENERATOR-type command: %s' % command_string)
            generator_spec = lookup_generator_command(command_string)
            return CommandInput(cmd_type='generator',
                                cmd_object=GeneratorCommand(cmd_string=command_string, cmdspec=generator_spec, modifiers=modifiers))

    print('looking up SMS command: %s' % command_string)
    cmd_spec = lookup_sms_command(command_string)
    if not cmd_spec:
        # we do not recognize this command
        raise UnrecognizedSMSCommand(command_string)
    
    return CommandInput(cmd_type='syscommand',
                        cmd_object=SystemCommand(job_tag=job_tag, cmdspec=cmd_spec, modifiers=modifiers))
    

def lookup_courier_by_mobile_number(mobile_number, session, db_svc):
    Courier = db_svc.Base.classes.couriers
    try:
        return session.query(Courier).filter(Courier.mobile_number == mobile_number).one()
    except:
        return None


def compile_help_string():
    lines = []
    for key, cmd_spec in SMS_SYSTEM_COMMAND_SPECS.items():
        lines.append('%s: %s' % (key, cmd_spec.definition))
    
    return '\n\n'.join(lines)


def handle_on_duty(cmd_object, dlg_context, service_registry, **kwargs):
    return '''
    Welcome to the on-call roster. Reply to job tags with the tag and "gtg" to accept a job. Text "hlp" or "?" at any time to see the command codes.'''


def handle_off_duty(cmd_object, dlg_context, service_registry, **kwargs):
    return 'You are now leaving the on-call roster. Thank you for your service. Have a good one!'


def handle_bid_for_job(cmd_object, dlg_context, service_registry, **kwargs):

    if not cmd_object.job_tag:
        return 'Bid for a job by texting the job tag, space, and "bid".' 

    if not is_valid_job_tag(cmd_object.job_tag):
        return REPLY_INVALID_TAG

    db_svc = service_registry.lookup('postgres')
    with db_svc.txn_scope() as session:
        # make sure the job is open
        if not job_is_available(cmd_object.job_tag, session, db_svc):
            return 'The job with tag:\n%s\n is not in the pool of available jobs.\nText "opn" for a list of open jobs.'
        
        # job exists and is available, so bid for it
        kwargs['job_tag'] = cmd_object.job_tag
        bid = ObjectFactory.create_job_bid(db_svc,
                                           courier_id=dlg_context.courier.id,
                                           job_tag=cmd_object.job_tag)
        session.add(bid)

        return '\n'.join([
            "Thank you! You've made a bid to accept job:",
            cmd_object.job_tag,
            "If you get the assignment, we'll text you when the bidding window closes."
        ])

    
def handle_accept_job(cmd_object, dlg_context, service_registry, **kwargs):
    # TODO: update status table
    return '<placeholder for accepting a job>'


def handle_help(cmd_object, dlg_context, service_registry, **kwargs):
    return compile_help_string()


def handle_job_details(cmd_object, dlg_context, service_registry, **kwargs):
    if cmd_object.job_tag == None:
        return 'To receive details on a job, text the job tag, a space, and "dt"'

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
    # TODO: update status table
    # TODO: handle missing job tag if courier has more than one job

    if not cmd_object.job_tag:
        return "Please text the tag of the job you're starting, space, then 'er'."
        
    lines = [
        "You have reported that you're en route for job:",
        cmd_object.get('job_tag')
    ]

    return '\n'.join(lines)


def handle_cancel_job(cmd_object, dlg_context, service_registry, **kwargs):
    # TODO: handle missing job tag
    # TODO: update status table
    return "Canceling job."
    # TODO: add cancel logic


def handle_job_finished(cmd_object, dlg_context, service_registry, **kwargs):
    # TODO: handle missing job tag
    # TODO: update status table
    return "Recording job completion for job tag %s. Thank you!" % cmd_object.job_tag


def handle_emergency(cmd_object, dlg_context, service_registry, **kwargs):
    # TODO: add broadcast logic
    return "Reporting an emergency for courier X. If this is a life-threatening emergency, please call 911"
    

def generate_list_my_jobs(cmd_object, dlg_engine, dlg_context, service_registry, **kwargs):
    print('Specifier character is "%s"' % cmd_object.cmdspec.specifier)

    tokens = cmd_object.cmd_string.split(cmd_object.cmdspec.specifier)

    if len(tokens) == 1:
        # show all my assigned jobs
        return "<placeholder for listing ALL jobs assigned to this courier>"

    else:
        # show a subset of my assigned jobs
        return "<placeholder for showing the Nth job assigned to this courier>"


def extension_is_integer(ext_string):
    if INTEGER_RX.match(ext_string):
        return True
    return False


def extension_is_range(ext_string):
    if RANGE_RX.match(ext_string):
        return True
    return False


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
            if extension_is_integer(ext):
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

            elif extension_is_range(ext):
                # if we receive <cmd><specifier>N-M where N and M are both integers, return the Nth through the Mth items

                return "<placeholder for showing N through M open jobs>"
                

SMSDialogContext = namedtuple('SMSDialogContext', 'courier source_number message')

class DialogEngine(object):
    def __init__(self):
        self.msg_dispatch_tbl = {}
        self.generator_dispatch_tbl = {}


    def register_cmd_spec(self, sms_command_spec, handler_func):
        self.msg_dispatch_tbl[str(sms_command_spec)] = handler_func


    def register_generator_cmd(self, generator_cmd_spec, handler_func):
        self.generator_dispatch_tbl[str(generator_cmd_spec)] = handler_func


    def _reply_hashtag_command(self, hashtag_cmd, dialog_context, service_registry, **kwargs):
        return 'placeholder for hashtag response'


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
        # command types: generator, syscommand, hashtag
        if command_input.cmd_type == 'hashtag':
            return self._reply_hashtag_command(command_input.cmd_object, dialog_context, service_registry)
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
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['acc'], handle_accept_job)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['dt'], handle_job_details)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['er'], handle_en_route)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['can'], handle_cancel_job)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['fin'], handle_job_finished)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['911'], handle_emergency)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['hlp'], handle_help)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['on'], handle_on_duty)
    engine.register_cmd_spec(SMS_SYSTEM_COMMAND_SPECS['off'], handle_off_duty)

    engine.register_generator_cmd(SMS_GENERATOR_COMMAND_SPECS['my'], generate_list_my_jobs)
    engine.register_generator_cmd(SMS_GENERATOR_COMMAND_SPECS['opn'], generate_list_open_jobs)

    print('###------ SMS payload:')
    source_number = input_data['From']
    message_body = input_data['Body']
    print('###------ Received message "%s" from mobile number [%s].' % (message_body, source_number))

    mobile_number = normalize_mobile_number(source_number)

    courier = None
    with db_svc.txn_scope() as session:
        courier = lookup_courier_by_mobile_number(mobile_number, session, db_svc)
        if not courier:
            print('Courier with mobile number %s not found.' % mobile_number)
            sms_svc.send_sms(mobile_number, REPLY_NOT_IN_NETWORK)
            return core.TransformStatus(ok_status('SMS event received', is_valid_command=False))
        
        session.expunge(courier)
        
    dlg_context = SMSDialogContext(courier=courier, source_number=mobile_number, message=message_body)

    try:
        command_input = parse_sms_message_body(message_body)
        print('#----- Resolved command: %s' % str(command_input))

        response = engine.reply_command(command_input, dlg_context, service_objects)
        sms_svc.send_sms(mobile_number, response)

        return core.TransformStatus(ok_status('SMS event received', is_valid_command=True, command=command_input))

    except UnrecognizedSMSCommand as err:
        print('#----- Unrecognized system command: in message body: %s' % message_body)
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


def active_job_bidders_func(input_data, service_objects, **kwargs):
    '''Return a list of couriers who have bid on the job passed in the input data,
    provided the job is active (calling this function against a job whose bidding window 
    has closed, should return an empty list)
    '''

    job_tag = input_data['job_tag']
    courier_list = []
    db_svc = service_objects.lookup('postgres')
    JobBid = db_svc.Base.classes.job_bids
    Courier = db_svc.Base.classes.couriers

    with db_svc.txn_scope() as session:
        for c, jb in session.query(Courier, JobBid).filter(and_(Courier.id == JobBid.courier_id,
                                                                JobBid.job_tag == job_tag,
                                                                JobBid.accepted_ts == None,
                                                                JobBid.expired_ts == None)).all(): 

            courier_list.append({
                'first_name': c.first_name,
                'last_name': c.last_name,
                'mobile_number': c.mobile_number
            })

    return core.TransformStatus(ok_status('get active job bidders', couriers=courier_list))


