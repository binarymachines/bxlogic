
#!/usr/bin/env python

#
# Generated Flask routing module for SNAP microservice framework
#



from flask import Flask, request, Response
from flask_cors import CORS, cross_origin
from snap import snap
from snap import core
import logging
import json
import argparse
import sys
from snap.loggers import request_logger as log

sys.path.append('/home/dtaylor/workshop/binary/bxlogic')

import bx_transforms 

f_runtime = Flask(__name__)

if __name__ == '__main__':
    print('starting SNAP microservice in standalone (debug) mode...')
    f_runtime.config['startup_mode'] = 'standalone'
    
else:
    print('starting SNAP microservice in wsgi mode...')
    f_runtime.config['startup_mode'] = 'server'

app = snap.setup(f_runtime)
xformer = core.Transformer(app.config.get('services'))


#-- exception handlers ---

xformer.register_error_code(snap.NullTransformInputDataException, snap.HTTP_BAD_REQUEST)
xformer.register_error_code(snap.MissingInputFieldException, snap.HTTP_BAD_REQUEST)
xformer.register_error_code(snap.TransformNotImplementedException, snap.HTTP_NOT_IMPLEMENTED)

#-- data shapes ----------

default = core.InputShape("default")
new_courier_shape = core.InputShape("new_courier_shape")
new_courier_shape.add_field('first_name', 'str', True)
new_courier_shape.add_field('last_name', 'str', True)
new_courier_shape.add_field('mobile_number', 'str', True)
new_courier_shape.add_field('email', 'str', True)
new_courier_shape.add_field('boroughs', 'str', True)
new_courier_shape.add_field('transport_methods', 'str', True)
update_courier_status_shape = core.InputShape("update_courier_status_shape")
update_courier_status_shape.add_field('id', 'str', True)
update_courier_status_shape.add_field('status', 'int', True)
couriers_by_status_shape = core.InputShape("couriers_by_status_shape")
couriers_by_status_shape.add_field('status', 'int', True)
new_client_shape = core.InputShape("new_client_shape")
new_client_shape.add_field('first_name', 'str', True)
new_client_shape.add_field('last_name', 'str', False)
new_client_shape.add_field('phone', 'str', True)
new_client_shape.add_field('email', 'str', False)
new_job_shape = core.InputShape("new_job_shape")
new_job_shape.add_field('client_id', 'str', True)
new_job_shape.add_field('delivery_address', 'str', True)
new_job_shape.add_field('delivery_borough', 'str', True)
new_job_shape.add_field('delivery_zip', 'str', True)
new_job_shape.add_field('delivery_neighborhood', 'str', False)
new_job_shape.add_field('pickup_address', 'str', True)
new_job_shape.add_field('pickup_borough', 'str', True)
new_job_shape.add_field('pickup_neighborhood', 'str', False)
new_job_shape.add_field('pickup_zip', 'str', True)
new_job_shape.add_field('payment_method', 'str', True)
new_job_shape.add_field('items', 'str', True)
new_job_shape.add_field('delivery_window_open', 'str', False)
new_job_shape.add_field('delivery_window_close', 'str', False)
poll_job_status_shape = core.InputShape("poll_job_status_shape")
poll_job_status_shape.add_field('job_tag', 'str', True)
update_job_log_shape = core.InputShape("update_job_log_shape")
update_job_log_shape.add_field('job_tag', 'str', True)
update_job_log_shape.add_field('message', 'str', True)
default = core.InputShape("default")
job_bidders_shape = core.InputShape("job_bidders_shape")
job_bidders_shape.add_field('job_tag', 'str', True)

#-- transforms ----

xformer.register_transform('ping', default, bx_transforms.ping_func, 'application/json')
xformer.register_transform('new_courier', new_courier_shape, bx_transforms.new_courier_func, 'application/json')
xformer.register_transform('update_courier_status', update_courier_status_shape, bx_transforms.update_courier_status_func, 'application/json')
xformer.register_transform('couriers_by_status', couriers_by_status_shape, bx_transforms.couriers_by_status_func, 'application/json')
xformer.register_transform('new_client', new_client_shape, bx_transforms.new_client_func, 'application/json')
xformer.register_transform('new_job', new_job_shape, bx_transforms.new_job_func, 'application/json')
xformer.register_transform('poll_job_status', poll_job_status_shape, bx_transforms.poll_job_status_func, 'application/json')
xformer.register_transform('update_job_log', update_job_log_shape, bx_transforms.update_job_log_func, 'application/json')
xformer.register_transform('sms_responder', default, bx_transforms.sms_responder_func, 'text/json')
xformer.register_transform('active_job_bidders', job_bidders_shape, bx_transforms.active_job_bidders_func, 'application/json')

#-- endpoints -----------------

@app.route('/ping', methods=['GET'])
def ping():
    try:
        if app.debug:
            # dump request headers for easier debugging
            log.info('### HTTP request headers:')
            log.info(request.headers)

        input_data = {}
                                
        input_data.update(request.args)
        
        transform_status = xformer.transform('ping',
                                             input_data,
                                             headers=request.headers)
                
        output_mimetype = xformer.target_mimetype_for_transform('ping')

        if transform_status.ok:
            return Response(transform_status.output_data, status=snap.HTTP_OK, mimetype=output_mimetype)
        return Response(json.dumps(transform_status.user_data), 
                        status=transform_status.get_error_code() or snap.HTTP_DEFAULT_ERRORCODE, 
                        mimetype=output_mimetype) 
    except Exception as err:
        log.error("Exception thrown: ", exc_info=1)        
        raise err

@app.route('/courier', methods=['POST'])
def new_courier():
    try:
        if app.debug:
            # dump request headers for easier debugging
            log.info('### HTTP request headers:')
            log.info(request.headers)

        input_data = {}
                
        request.get_data()
        input_data.update(core.map_content(request))
        
        transform_status = xformer.transform('new_courier', input_data, headers=request.headers)

                
        output_mimetype = xformer.target_mimetype_for_transform('new_courier')

        if transform_status.ok:
            return Response(transform_status.output_data, status=snap.HTTP_OK, mimetype=output_mimetype)
        return Response(json.dumps(transform_status.user_data), 
                        status=transform_status.get_error_code() or snap.HTTP_DEFAULT_ERRORCODE, 
                        mimetype=output_mimetype) 
    except Exception as err:
        log.error("Exception thrown: ", exc_info=1)        
        raise err

@app.route('/courier-status', methods=['POST'])
def update_courier_status():
    try:
        if app.debug:
            # dump request headers for easier debugging
            log.info('### HTTP request headers:')
            log.info(request.headers)

        input_data = {}
                
        request.get_data()
        input_data.update(core.map_content(request))
        
        transform_status = xformer.transform('update_courier_status', input_data, headers=request.headers)

                
        output_mimetype = xformer.target_mimetype_for_transform('update_courier_status')

        if transform_status.ok:
            return Response(transform_status.output_data, status=snap.HTTP_OK, mimetype=output_mimetype)
        return Response(json.dumps(transform_status.user_data), 
                        status=transform_status.get_error_code() or snap.HTTP_DEFAULT_ERRORCODE, 
                        mimetype=output_mimetype) 
    except Exception as err:
        log.error("Exception thrown: ", exc_info=1)        
        raise err

@app.route('/couriers', methods=['GET'])
def couriers_by_status():
    try:
        if app.debug:
            # dump request headers for easier debugging
            log.info('### HTTP request headers:')
            log.info(request.headers)

        input_data = {}
                                
        input_data.update(request.args)
        
        transform_status = xformer.transform('couriers_by_status',
                                             input_data,
                                             headers=request.headers)
                
        output_mimetype = xformer.target_mimetype_for_transform('couriers_by_status')

        if transform_status.ok:
            return Response(transform_status.output_data, status=snap.HTTP_OK, mimetype=output_mimetype)
        return Response(json.dumps(transform_status.user_data), 
                        status=transform_status.get_error_code() or snap.HTTP_DEFAULT_ERRORCODE, 
                        mimetype=output_mimetype) 
    except Exception as err:
        log.error("Exception thrown: ", exc_info=1)        
        raise err

@app.route('/client', methods=['POST'])
def new_client():
    try:
        if app.debug:
            # dump request headers for easier debugging
            log.info('### HTTP request headers:')
            log.info(request.headers)

        input_data = {}
                
        request.get_data()
        input_data.update(core.map_content(request))
        
        transform_status = xformer.transform('new_client', input_data, headers=request.headers)

                
        output_mimetype = xformer.target_mimetype_for_transform('new_client')

        if transform_status.ok:
            return Response(transform_status.output_data, status=snap.HTTP_OK, mimetype=output_mimetype)
        return Response(json.dumps(transform_status.user_data), 
                        status=transform_status.get_error_code() or snap.HTTP_DEFAULT_ERRORCODE, 
                        mimetype=output_mimetype) 
    except Exception as err:
        log.error("Exception thrown: ", exc_info=1)        
        raise err

@app.route('/job', methods=['POST'])
def new_job():
    try:
        if app.debug:
            # dump request headers for easier debugging
            log.info('### HTTP request headers:')
            log.info(request.headers)

        input_data = {}
                
        request.get_data()
        input_data.update(core.map_content(request))
        
        transform_status = xformer.transform('new_job', input_data, headers=request.headers)

                
        output_mimetype = xformer.target_mimetype_for_transform('new_job')

        if transform_status.ok:
            return Response(transform_status.output_data, status=snap.HTTP_OK, mimetype=output_mimetype)
        return Response(json.dumps(transform_status.user_data), 
                        status=transform_status.get_error_code() or snap.HTTP_DEFAULT_ERRORCODE, 
                        mimetype=output_mimetype) 
    except Exception as err:
        log.error("Exception thrown: ", exc_info=1)        
        raise err

@app.route('/jobstatus', methods=['GET'])
def poll_job_status():
    try:
        if app.debug:
            # dump request headers for easier debugging
            log.info('### HTTP request headers:')
            log.info(request.headers)

        input_data = {}
                                
        input_data.update(request.args)
        
        transform_status = xformer.transform('poll_job_status',
                                             input_data,
                                             headers=request.headers)
                
        output_mimetype = xformer.target_mimetype_for_transform('poll_job_status')

        if transform_status.ok:
            return Response(transform_status.output_data, status=snap.HTTP_OK, mimetype=output_mimetype)
        return Response(json.dumps(transform_status.user_data), 
                        status=transform_status.get_error_code() or snap.HTTP_DEFAULT_ERRORCODE, 
                        mimetype=output_mimetype) 
    except Exception as err:
        log.error("Exception thrown: ", exc_info=1)        
        raise err

@app.route('/joblog', methods=['POST'])
def update_job_log():
    try:
        if app.debug:
            # dump request headers for easier debugging
            log.info('### HTTP request headers:')
            log.info(request.headers)

        input_data = {}
                
        request.get_data()
        input_data.update(core.map_content(request))
        
        transform_status = xformer.transform('update_job_log', input_data, headers=request.headers)

                
        output_mimetype = xformer.target_mimetype_for_transform('update_job_log')

        if transform_status.ok:
            return Response(transform_status.output_data, status=snap.HTTP_OK, mimetype=output_mimetype)
        return Response(json.dumps(transform_status.user_data), 
                        status=transform_status.get_error_code() or snap.HTTP_DEFAULT_ERRORCODE, 
                        mimetype=output_mimetype) 
    except Exception as err:
        log.error("Exception thrown: ", exc_info=1)        
        raise err

@app.route('/sms', methods=['POST'])
def sms_responder():
    try:
        if app.debug:
            # dump request headers for easier debugging
            log.info('### HTTP request headers:')
            log.info(request.headers)

        input_data = {}
                
        request.get_data()
        input_data.update(core.map_content(request))
        
        transform_status = xformer.transform('sms_responder', input_data, headers=request.headers)

                
        output_mimetype = xformer.target_mimetype_for_transform('sms_responder')

        if transform_status.ok:
            return Response(transform_status.output_data, status=snap.HTTP_OK, mimetype=output_mimetype)
        return Response(json.dumps(transform_status.user_data), 
                        status=transform_status.get_error_code() or snap.HTTP_DEFAULT_ERRORCODE, 
                        mimetype=output_mimetype) 
    except Exception as err:
        log.error("Exception thrown: ", exc_info=1)        
        raise err

@app.route('/bidders', methods=['GET'])
def active_job_bidders():
    try:
        if app.debug:
            # dump request headers for easier debugging
            log.info('### HTTP request headers:')
            log.info(request.headers)

        input_data = {}
                                
        input_data.update(request.args)
        
        transform_status = xformer.transform('active_job_bidders',
                                             input_data,
                                             headers=request.headers)
                
        output_mimetype = xformer.target_mimetype_for_transform('active_job_bidders')

        if transform_status.ok:
            return Response(transform_status.output_data, status=snap.HTTP_OK, mimetype=output_mimetype)
        return Response(json.dumps(transform_status.user_data), 
                        status=transform_status.get_error_code() or snap.HTTP_DEFAULT_ERRORCODE, 
                        mimetype=output_mimetype) 
    except Exception as err:
        log.error("Exception thrown: ", exc_info=1)        
        raise err



if __name__ == '__main__':
    #
    # If we are loading from command line,
    # run the Flask app explicitly
    #
    app.run(host='0.0.0.0', port=9050)

