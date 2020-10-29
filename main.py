# packages
import os
import jwt
import json
import time
import hashlib
import requests
import numpy  as np
import pandas as pd

# project
from helpers.authenticate import authenticate_service_account

# API interface
API_URL_BASE = os.environ.get('API_URL_BASE')
EMU_URL_BASE = os.environ.get('EMU_URL_BASE')
AUTH_ENDPOINT = os.environ.get('AUTH_ENDPOINT')

# studio labels
EMULATION_LABEL       = 'inertia-model'
TWIN_NAME_APPENDIX    = ' twin'
ORIGINAL_DEVICE_LABEL = 'original_device_id'

# environment
DT_SIGNATURE_HEADER     = "x-dt-signature"
DT_SIGNATURE_SECRET     = os.environ.get("DT_SIGNATURE_SECRET")
SERVICE_ACCOUNT_EMAIL   = os.environ.get('SERVICE_ACCOUNT_EMAIL')
SERVICE_ACCOUNT_KEY_ID  = os.environ.get('SERVICE_ACCOUNT_KEY_ID')
SERVICE_ACCOUNT_SERCRET = os.environ.get('SERVICE_ACCOUNT_SERCRET')


def convert_event_data_timestamp(ts):
    """
    Convert the default event_data timestamp format to Pandas and unixtime format.

    Parameters
    ----------
    ts : str
        UTC timestamp in custom API event data format.

    Returns
    -------
    timestamp : datetime
        Pandas Timestamp object format.
    unixtime : int
        Integer number of seconds since 1 January 1970.
    """

    timestamp = pd.to_datetime(ts)
    unixtime  = pd.to_datetime(np.array([ts])).astype(int)[0] // 10**9

    return timestamp, unixtime


def update_emulated_twin(event, twin, coefficient, project_id, access_token):
    """
    Update the modeled temperature value of the emulated twin device.

    Parameters
    ----------
    event : dict
        Dictionary of new event data received by HTTP GET request call.
    twin : dict
        Dictionary of emulated twin device information.
    project_id : str
        Identifier of project we're interfacing with.
    access_token : str
        Acces token received from DT authentication endpoint.

    Returns
    -------
    status_code : tuple
        Two-cell tuple with status message [0] and status code [1].

    """

    # verify coefficient label exists
    try:
        k = float(coefficient)
    except ValueError:
        return ('-- non-float coefficient, skipping...', 200)

    # isolate event temperature value
    event_temperature = event['data']['temperature']['value']

    # if None, no previous events have occured, set equal to current
    if 'reported' not in twin.keys() or twin['reported']['temperature'] == None:
        new_value = event_temperature
    else:
        # fetch previous model temperature value
        previous_model_temperature = twin['reported']['temperature']['value']

        # get time since last event
        _, previous_model_ux = convert_event_data_timestamp(twin['reported']['temperature']['updateTime'])
        _, event_ux          = convert_event_data_timestamp(event['data']['temperature']['updateTime'])

        # normalise by the minute
        normaliser = (event_ux - previous_model_ux) / (60)

        # calculate new model delta
        dt = -k*(previous_model_temperature - event_temperature)

        # set new value
        new_value = previous_model_temperature + dt*normaliser
        
    # emit new value to emulated twin
    twin_id = twin['name'].split('/')[-1]
    emulator_emit_url = "{}/projects/{}/devices/{}:publish".format(EMU_URL_BASE, project_id, twin_id)
    payload = json.dumps({"temperature": {"value": new_value}})
    r = requests.post(emulator_emit_url, headers={'Authorization': access_token}, data=payload)
    if int(r.status_code) != 200:
        return ('ERROR: bad emit response', int(r.status_code))

    print('-- Emitted new value to twin.')
    return ('OK', 200)


def get_device_name(device):
    """
    Find the name of a device fetched from the project device list.
    Returns the device identifier if no name is explicitly given.

    Parameters
    ----------
    device : dict
        Dictionary of device information fetched by the API.

    Returns
    -------
    name : string
        Given name or identifier of device.

    """

    # check if label name exists
    if 'name' in device['labels'].keys():
        return device['labels']['name']
    else:
        return device['name'].split('/')[-1]


def find_twin(device_id, device_list):
    """
    Locate the emulated twin of source device with identifier device_id.

    Parameters
    ----------
    device_id : str
        Identifier of original device for which the twin is located.
    device_list : list
        List of device dictionaries in project fetched by the API.

    Returns
    -------
    twin : dict
        Dictionary of device information for located twin.
        Returns None if no twin were found.

    """

    # iterate devices
    for device in device_list:
        # skip non-emulated devices
        if not device['name'].split('/')[-1].startswith('emu'):
            continue

        # check if device_id label exists and matches
        if 'original_device_id' in device['labels'].keys() and device['labels']['original_device_id'] == device_id:
            print('-- Located twin [{}].'.format(device['labels']['name']))
            return device

    # no twin found
    return None


def clean_twins(device_id, device_list, project_id, access_token):
    """
    Remove any emulated twins related to original device with identifier device_id.

    Parameters
    ----------
    device_id : str
        Identifier of original device for which twins are cleaned.
    device_list : list
        List of project device dictionaries fetched by the API.
    project_id : str
        Identifier of the project we're interfacing with.
    access_token : str
        Acces token received from DT authentication endpoint.

    Returns
    -------
    status : tuple
        Tuple with 2 cells containing status text [0] and status code [1].

    """

    # iterate devices
    for device in device_list:
        # skip non-emulated devices
        if not device['name'].split('/')[-1].startswith('emu'):
            continue

        # check if device_id label exists
        if 'original_device_id' in device['labels'].keys() and device['labels']['original_device_id'] == device_id:
            # isolate id of device to be deleted
            delete_id = device['name'].split('/')[-1]

            # send delete request
            emulator_delete_url = "{}/projects/{}/devices/{}".format(EMU_URL_BASE, project_id, delete_id)
            r = requests.delete(emulator_delete_url, headers={'Authorization': access_token})

            # verify deletion
            if r.status_code == 200:
                print('-- deleted twin: {}'.format(device['labels']['name']))
            else:
                print('WARNING: could not delete twin: {}'.format(device_id))


def spawn_twin(device_id, original_name, project_id, access_token):
    """
    Spawn new emulated twin for original device with identifier device_id.

    Parameters
    ----------
    device_id : str
        Identifier of original device for which twins are cleaned.
    original_name : str
        Given name or identifier of original device.
    project_id : str
        Identifier of the project we're interfacing with.
    access_token : str
        Acces token received from DT authentication endpoint.

    Returns
    -------
    status_code : int
        Status code of POST request when creating new twin.
    twin : dict
        Dictionary of newly spawned twin.

    """

    twin_name = original_name + TWIN_NAME_APPENDIX
    emulator_emit_url = "{}/projects/{}/devices".format(EMU_URL_BASE, project_id)
    payload = json.dumps({
        'type': 'temperature',
        'labels': {
            'name': twin_name,
            ORIGINAL_DEVICE_LABEL: device_id,
        }
    })
    r = requests.post(emulator_emit_url, headers={'Authorization': access_token}, data=payload)

    if r.status_code == 200:
        print('-- Spawned twin [{}].'.format(twin_name))

    return r.status_code, r.json()


def find_original_device(device_id, device_list):
    """
    Locate the dictionary of original device in list fetched by API.

    Parameters
    ----------
    device_id : str
        Identifier of original device we're looking for.
    device_list : list
        List of project device dictionaries fetched by API.

    Returns
    -------
    device : dict
        Dictionary of original device located.
        Returns None if no device is found.

    """

    # iterate devices
    for device in device_list:
        # match id
        if device_id == device['name'].split('/')[-1]:
            # found it
            return device

    # no device found
    return None


def refresh_twin_name(twin, new_prefix, project_id, access_token):
    """
    Update given name of emulated twin.

    Parameters
    ----------
    twin : dict
        Dictionary of device information for twin.
    new_prefix : str
        New naming prefix to be used in new name.
    project_id : str
        Identifier of the project we're interfacing with.
    access_token : str
        Acces token received from DT authentication endpoint.

    """

    # emit new value to emulated twin
    twin_id = twin['name'].split('/')[-1]
    emulator_emit_url = "{}/projects/{}/devices/{}/labels/name?updateMask=value".format(API_URL_BASE, project_id, twin_id)
    payload = json.dumps({'value': new_prefix + TWIN_NAME_APPENDIX})
    r = requests.patch(emulator_emit_url, headers={'Authorization': access_token}, data=payload)
    if r.status_code == 200:
        print('-- Twin name refresh: {} -> {}.'.format(twin['labels']['name'], new_prefix + TWIN_NAME_APPENDIX))
    else:
        print('-- WARNING: Could not change name.')


def synchronize_emulated_twin(event, labels, device_id, device_list, project_id, access_token):
    """
    event : dict
        Dictionary form of new event json received from request.
    labels : dict
        Dictionary of labels in new event json received from request.
    device_id : str
        Identifier of target device for new event.
    device_list : list
        List of project device dictionaries fetched by API.
    project_id : str
        Identifier of the project we're interfacing with.
    access_token : str
        Acces token received from DT authentication endpoint.

    Returns
    -------
    status : tuple
        Tuple with 2 cells containing status text [0] and status code [1].
    twin : dict
        Dictionary of emulated twin for target device in event.

    """

    # initialise some helper variables
    twin = None
    new_spawn = False

    # check for labelsChanged event
    if event['eventType'] == 'labelsChanged':
        # new label
        if EMULATION_LABEL in event['data']['added'].keys():
            new_spawn = True
            print('-- New emulation label.')

        # modified label
        elif EMULATION_LABEL in event['data']['modified'].keys():
            return ('-- Modified emulation label.', 200), None

        # removed label
        elif EMULATION_LABEL in event['data']['removed']:
            # remove any emulated devices associated with event source device
            clean_twins(device_id, device_list, project_id, access_token)
            return ('-- Removed emulation label.', 200), None

    if EMULATION_LABEL in labels.keys() or new_spawn:
        # find twin if it exists
        twin = find_twin(device_id, device_list)
    
        # locate original device in device_list
        original_device = find_original_device(device_id, device_list)
    
        # verify that we found original device in device list
        if original_device == None: 
            return ('could not find original device', 400), None
    
        # if it wasn't found (None), spawn it
        if twin == None:
            # cleanup existing twins
            clean_twins(device_id, device_list, project_id, access_token)
    
            # spawn new twin
            spawn_status, twin = spawn_twin(device_id, get_device_name(original_device), project_id, access_token)
    
            # verify good spawn
            if spawn_status != 200:
                return ('ERROR: could not spawn twin', 400), None
    
        # refresh twin name
        if twin != None:
            if not get_device_name(twin).startswith(get_device_name(original_device)):
                refresh_twin_name(twin, get_device_name(original_device), project_id, access_token)
    
        print('-- Synchronized with twin.')
        return ('OK', 200), twin
    
    else:
        # remove any emulated devices associated with event source device
        clean_twins(device_id, device_list, project_id, access_token)
        return ('no emulation label', 200), None



def api_interface(event, labels, access_token):
    """
    Talk to API to calculate new model value.
    Filters events and defines the order of action.
    Always performs emulated twin synchronization first.

    Parameters
    ----------
    event : dict
        Dictionary form of new event json received from request.
    labels : dict
        Dictionary of labels in new event json received from request.
    access_token : str
        Acces token received from DT authentication endpoint.

    Returns
    -------
    status : tuple
        Tuple with 2 cells containing status text [0] and status code [1].

    """

    # skip non-temperature events
    if event['eventType'] != 'temperature' and event['eventType'] != 'labelsChanged':
        return ('skipped event type {}'.format(event['eventType']), 200)

    # skip sensors with no emulation label
    # if EMULATION_LABEL not in labels.keys() and event['eventType'] != 'labelsChanged':
    #     return ('no emulation', 200)

    # request project devices list
    project_id       = event['targetName'].split('/')[1]
    devices_list_url = "{}/projects/{}/devices".format(API_URL_BASE, project_id)
    device_list      = requests.get(devices_list_url, headers={'Authorization': access_token}).json()['devices']
    device_id        = event['targetName'].split('/')[-1]

    # synchronize
    status, twin = synchronize_emulated_twin(event, labels, device_id, device_list, project_id, access_token)

    # verify status
    if status[1] != 200 or twin == None:
        return status

    # stop here if labelchange event
    if event['eventType'] == 'labelsChanged':
        return ('OK', 200)

    # calculate model delta T
    status = update_emulated_twin(event, twin, labels[EMULATION_LABEL], project_id, access_token)
    if status[1] != 200:
        return status
    
    return ('OK', 200)


def project_validate(request):
    """
    Perform OAuth2 authentication flow for DT Authentication.
    Uses service accounts for access control.
    Uses JWT as the medium for the exchange.
    https://support.disruptive-technologies.com/hc/en-us/articles/360011534099-Authentication

    Parameters
    ----------
    request : dictionary
        HTTP POST request received.

    Returns
    -------
    status : tuple
        Tuple with 2 cells containing status text [0] and status code [1].

    """

    # check for signature environment variable
    if DT_SIGNATURE_SECRET == None:
        return ('missing secret', 400)

    # check for dt header in request
    if DT_SIGNATURE_HEADER not in request.headers:
        return ('missing header', 400)

    # verify secret against environment variable
    token = request.headers[DT_SIGNATURE_HEADER]
    try:
        payload = jwt.decode(token, DT_SIGNATURE_SECRET, algorithms=["HS256"])
    except:
        return ('signature error', 400)

    # verify body checksum
    m = hashlib.sha1()
    m.update(request.get_data())
    checksum = m.digest().hex()

    if payload["checksum"] != checksum:
        return ('checksum mismatch', 400)

    # success
    return ('OK', 200)


def terminate(status, dt):
    print('-- Execution ended at {:.3f}s with status {}.'.format(dt, status))

    # logging frame end
    print('END' + '-'*50)

    return status


def dataconnector_endpoint(request):
    """
    Point of contact with dataconnector.
    Validates the request and authenticates for API.
    Executes function for updating model temperature.

    Parameters
    ----------
    request : dictionary
        HTTP POST request received.

    Returns
    -------
    status : tuple
        Tuple with 2 cells containing status text [0] and status code [1].

    """

    # time the execution
    start = time.time()

    # logging frame start
    print('START' + '-'*50)

    # validate secret etc
    status = project_validate(request)
    if status[1] != 200:
        return terminate(status, time.time()-start)

    # authenticate to service account
    access_token = authenticate_service_account(SERVICE_ACCOUNT_EMAIL,
                                                SERVICE_ACCOUNT_KEY_ID,
                                                SERVICE_ACCOUNT_SERCRET,
                                                AUTH_ENDPOINT)
    if access_token == None:
        return terminate(('Not Authenticated', 401), time.time()-start)

    # talk to api
    status = api_interface(request.get_json()['event'], request.get_json()['labels'], access_token)

    # success
    return terminate(status, time.time()-start)

