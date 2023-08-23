#
# oci-splunk-observability version 1.0.
#
# Copyright (c) 2022, Oracle and/or its affiliates. All rights reserved.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

import io
import json
import logging
import os
import requests

"""
# See https://docs.splunk.com/Documentation/Splunk/9.1.0/Data/HECExamples
"""

# Use OCI Application or Function configurations to override these environment variable defaults.

api_endpoint = os.getenv('SPLUNK_HEC_ENDPOINT', 'not-configured')
api_key = os.getenv('SPLUNK_HEC_TOKEN', 'not-configured')
is_forwarding = eval(os.getenv('FORWARDING_ENABLED', "True"))
batch_size = int(os.getenv('BATCH_SIZE', '1000'))

# NOTE: API Contact --
# "event" key is required
# "sourcetype" is optional

payload_map_default_works = {
    "event": "event",
    "sourcetype": "sourcetype"
}

payload_map_default = {
    "event": "name",
    "fields": {
        "time": "timestamp",
        "source": "name",
        "value": "value",
        "compartmentid": "compartmentid",
        "ingestedtime": "ingestedtime",
        "loggroupid": "loggroupid",
        "logid": "logid",
        "tenantid": "tenantid"
    }
}

payload_map_default_hold = {
    "time": "timestamp",
    "event": "name",
    "value": "value",
    "type": "type",
    "fields": {
        "region": None,
        "datacenter": None,
        "rack": None,
        "sourceAddress": 'sourceAddress',
        "displayName": "displayName",
        "namespace": 'namespace',
        "datetime": 'datetime',
        "resourceId": "resourceId",
        "service_version": None,
        "service_environment": None,
        "path": None,
        "fstype": None,
        "compartmentId": "compartmentId",
        "compartmentid": "compartmentid"
    }
}

payload_map = os.getenv('PAYLOAD_MAP', payload_map_default)

# Set all registered loggers to the configured log_level

logging_level = os.getenv('LOGGING_LEVEL', 'INFO')
loggers = [logging.getLogger()] + [logging.getLogger(name) for name in logging.root.manager.loggerDict]
[logger.setLevel(logging.getLevelName(logging_level)) for logger in loggers]


# Functions


def handler(ctx, data: io.BytesIO = None):
    """
    OCI Function Entry Point
    :param ctx: InvokeContext
    :param data: data payload
    :return: plain text response indicating success or error
    """

    preamble = " {} / event count = {} / logging level = {} / forwarding to endpoint = {}"

    try:
        event_list = json.loads(data.getvalue())
        logging.getLogger().info(preamble.format(ctx.FnName(), len(event_list), logging_level, is_forwarding))
        logging.getLogger().debug(event_list)
        converted_event_list = handle_events(event_list=event_list)
        send_to_endpoint(event_list=converted_event_list)

    except (Exception, ValueError) as ex:
        logging.getLogger().error('error handling logging payload: {}'.format(str(ex)))


def handle_events(event_list):
    """
    """

    if isinstance(event_list, dict):
        event_list = [event_list]

    result_list = []
    for event in event_list:

        is_metric = event.get('datapoints')
        if is_metric:
            result_list.extend(transform_metric(event))
        else:
            result_list.extend(transform_log(event))

    return result_list


def transform_log(record: dict):
    return [get_transformer()(record=record)]


def transform_metric(record: dict) -> list:

    results = []
    datapoints = record.get('datapoints')
    del record['datapoints']

    for point in datapoints:
        point["inversion"] = record
        result = get_transformer()(record=point)
        results.append(result)

    return results


def get_transformer():
    if payload_map is None:
        return transform_bypass
    else:
        return transform_using_map


def transform_using_map(record: dict, lookup_map=payload_map):
    """
    """

    result = {}
    for key, lookup in lookup_map.items():

        # recursive call here to pick up nested lookups

        if isinstance(lookup, dict):
            value = transform_using_map(record=record, lookup_map=lookup)
        else:
            value = get_dictionary_value(record, lookup)

        if value is not None:
            result[key] = value

    return result


def transform_bypass(record: dict):
    """
    See https://docs.splunk.com/Documentation/Splunk/9.1.0/Data/HECExamples
    """

    result = {
        'sourcetype': '_json',
        'event': record
    }

    return result


def send_to_endpoint(event_list):
    """
    """

    if is_forwarding is False:
        logging.getLogger().debug("forwarding is disabled - nothing sent")
        return

    # creating a session and adapter to avoid recreating
    # a new connection pool between each POST call

    session = requests.Session()

    try:
        http_headers = {'Content-type': 'application/json', 'Authorization': f'Splunk {api_key}'}
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
        session.mount('https://', adapter)

        # subdivide incoming payload into separate lists that are within the configured batch size

        batch_mode = True

        if batch_mode:
            batches = []
            sub_list = []
            batches.append(sub_list)

            for event in event_list:
                sub_list.append(event)
                if len(sub_list) > batch_size:
                    sub_list = []
                    batches.append(sub_list)

            for batch_list in batches:
                post_response = session.post(api_endpoint, data=json.dumps(batch_list), headers=http_headers, verify=False)

                if post_response.status_code != 200:
                    raise Exception('error posting to API endpoint', post_response.text)

        else:

            for event in event_list:
                post_response = session.post(api_endpoint, data=json.dumps(event), headers=http_headers, verify=False)

                if post_response.status_code != 200:
                    raise Exception('error posting to API endpoint', post_response.text)

    finally:
        session.close()


def get_dictionary_value(dictionary: dict, target_key: str):
    """
    Recursive method to find value within a dictionary which may also have nested lists / dictionaries.
    :param dictionary: the dictionary to scan
    :param target_key: the key we are looking for
    :return: If a target_key exists multiple times in the dictionary, the first one found will be returned.
    """

    if dictionary is None:
        raise Exception('dictionary None for key'.format(target_key))

    target_value = dictionary.get(target_key)
    if target_value is not None:
        return target_value

    for key, value in dictionary.items():
        if isinstance(value, dict):
            target_value = get_dictionary_value(dictionary=value, target_key=target_key)
            if target_value is not None:
                return target_value

        elif isinstance(value, list):
            for entry in value:
                if isinstance(entry, dict):
                    target_value = get_dictionary_value(dictionary=entry, target_key=target_key)
                    if target_value is not None:
                        return target_value


def local_test_mode_linefeed_file(filename):
    """
    This routine reads a local json metrics file, converting the contents to DataDog format.
    :param filename: cloud events json file exported from OCI Logging UI or CLI.
    :return: None
    """

    logging.getLogger().info("local testing started")

    inbound_events = list()
    with open(filename, 'r') as f:
        for line in f:
            event = json.loads(line)
            logging.getLogger().debug(json.dumps(event, indent=4))
            inbound_events.append(event)

    transformed_results = handle_events(event_list=inbound_events)
    print(json.dumps(transformed_results, indent=4))
    send_to_endpoint(event_list=transformed_results)

    logging.getLogger().info("local testing completed")


def local_test_mode_json_file(filename):

    with open(filename, 'r') as f:
        inbound_events = json.load(f)

    transformed_results = handle_events(event_list=inbound_events)
    print(json.dumps(transformed_results, indent=4))
    send_to_endpoint(event_list=transformed_results)
    logging.getLogger().info("local testing completed")


"""
Local Debugging 
"""

if __name__ == "__main__":
    # local_test_mode_linefeed_file('oci-metrics-test-file.json')
    local_test_mode_json_file('oci_logs.json')
    # local_test_mode_linefeed_file('simple-event.json') WORKS
