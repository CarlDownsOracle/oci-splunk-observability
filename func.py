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
See 
    https://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector
    https://docs.splunk.com/Documentation/Splunk/latest/Data/HECExamples
"""

# The Payload map is an optional feature that lets you precisely control mapping of OCI event
# attributes to the Splunk fields.  The Payload map is simple JSON where non-object r-values are
# used as the OCI event keys to extract.
#
# Note that the r-values can be at any nested level within the OCI event payload.
# This approach works for OCI logging events as well as OCI raw metrics events.


payload_map_default = """
{
    "fields": {
        "name": "name",
        "namespace": "namespace",
        "timestamp": "timestamp",
        "value": "value",
        "count": "count",
        "type": "type",
        "source": "source",
        "displayName": "displayName",
        "compartmentid": "compartmentid",
        "ingestedtime": "ingestedtime",
        "sourceAddress": "sourceAddress",
        "destinationAddress": "destinationAddress",
        "tenantid": "tenantid"
    }
}
"""

# Use OCI Application or Function configurations to override these environment variable defaults.
# SPLUNK_HEC_ENDPOINT trial account example:  https://<your-subdomain>.splunkcloud.com:8088/services/collector

api_endpoint = os.getenv('SPLUNK_HEC_ENDPOINT', 'not-configured')
api_key = os.getenv('SPLUNK_HEC_TOKEN', 'not-configured')
send_to_splunk = eval(os.getenv('SEND_TO_SPLUNK', "True"))
verify_ssl = eval(os.getenv('VERIFY_SSL', "True"))
batch_size = int(os.getenv('BATCH_SIZE', '100'))
payload_map = json.loads(os.getenv('PAYLOAD_MAP', payload_map_default))
bypass_payload_map = eval(os.getenv('BYPASS_PAYLOAD_MAP', "True"))

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
        logging.getLogger().info(preamble.format(ctx.FnName(), len(event_list), logging_level, send_to_splunk))
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
    """
    :param record: OCI Log event
    :return: record list transformed to Splunk format
    """
    return [get_transformer()(record=record)]


def transform_metric(record: dict) -> list:
    """
    In the case of OCI raw metrics, there can be several datapoints in one event.  To handle this,
    datapoints are re-characterized to map each one as individual Splunk events.
    :param record: OCI Raw Metric event
    :return: record list transformed to Splunk format
    """

    results = []
    datapoints = record.get('datapoints')
    del record['datapoints']

    for point in datapoints:
        point["details"] = record
        result = get_transformer()(record=point)
        results.append(result)

    return results


def get_transformer():
    """
    :return: the module function that performs the transformation depending on whether mapping is bypassed.
    """

    if bypass_payload_map is True:
        return transform_bypass
    else:
        return transform_using_map


def transform_using_map(record: dict, lookup_map=payload_map):
    """
    :param record: OCI Log or Raw Metric
    :param lookup_map: map to use for transformation
    :return: record transformed to Splunk format
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
    :return: record transformed to Splunk format
    """

    result = {
        'sourcetype': '_json',
        'event': record
    }

    return result


def send_to_endpoint(event_list):
    """
    :param event_list: list of transformed event records to send
    :return: None
    """

    if send_to_splunk is False:
        logging.getLogger().debug("forwarding to Splunk is disabled - nothing sent")
        return

    # creating a session and adapter to avoid recreating
    # a new connection pool between each POST call

    session = requests.Session()

    try:
        http_headers = {'Content-type': 'application/json', 'Authorization': f'Splunk {api_key}'}
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
        session.mount('https://', adapter)

        # subdivide incoming payload into separate lists that are within the configured batch size

        batches = []
        sub_list = []
        batches.append(sub_list)

        for event in event_list:
            sub_list.append(event)
            if len(sub_list) > batch_size:
                sub_list = []
                batches.append(sub_list)

        for batch_list in batches:
            post_response = session.post(api_endpoint,
                                         data=json.dumps(batch_list),
                                         headers=http_headers,
                                         verify=verify_ssl)

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
    This routine reads a local file with CR/LF separated event records, transforms and sends to Splunk if so enabled.
    :param filename: cloud events file exported from OCI UI or CLI.
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
    """
    This routine reads a local JSON file of event records, transforms and sends to Splunk if so enabled.
    :param filename: cloud events JSON file exported from OCI UI or CLI.
    :return: None
    """

    with open(filename, 'r') as f:
        inbound_events = json.load(f)

    transformed_results = handle_events(event_list=inbound_events)
    print(json.dumps(transformed_results, indent=4))
    send_to_endpoint(event_list=transformed_results)
    logging.getLogger().info("local testing completed")


"""
Local Testing 
"""

if __name__ == "__main__":
    local_test_mode_linefeed_file('oci-metrics-test-file.json')
    local_test_mode_json_file('oci_logs.json')

