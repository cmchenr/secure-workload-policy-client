"""
Copyright (c) 2021 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Cisco Sample
Code License, Version 1.1 (the "License"). You may obtain a copy of the
License at

               https://developer.cisco.com/docs/licenses

All use of the material herein must be in accordance with the terms of
the License. All rights not expressly granted by the License are
reserved. Unless required by applicable law or agreed to separately in
writing, software distributed under the License is distributed on an "AS
IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
or implied.
"""

__author__ = "Chris McHenry"
__copyright__ = "Copyright (c) 2021 Cisco and/or its affiliates."
__license__ = "Cisco Sample Code License, Version 1.1"


from kafka import KafkaConsumer
import ssl

import netaddr
import tetration_network_policy_pb2 as tnp_pb2
import ipaddress
import yaml
import json
import argparse
from netaddr import IPRange, IPSet

DEBUG = False
API_VERSION = (0, 9)  # Required by KafkaConsumer, refer to SDK docs
SSL = 'SSL'
KAFKA_CONSUMER_CA = 'KafkaConsumerCA.cert'
KAFKA_PRIVATE_KEY = 'KafkaConsumerPrivateKey.key'
AUTOCOMMIT = True
MAX_PARTITION_FETCH_BYTES = 1024 * 1024  # Default: 1M
ACTION = {1: 'ACCEPT', 2: 'DROP'}


class PolicySet(object):
    """
    Container for all messages that comprise a Network Policy, stores the Kafka Protocol Buffer
    """

    def __init__(self):
        """
        """
        self.result = {}
        self.inventory_filters = {}
        self.version = 0
        self.create_timestamp = 0
        self.scopes = {}
        self.intents = []
        self.catch_all = ""
        self.buffer = None
        self.update_end_offset = None
        self.start_at_offset = None


def create_ssl_context(cert_folder):
    """
    Our Tetration cluster was created using a self-signed certificate.
    KafkaConsumer provides a means to provide our own SSL context, enabling
    CERT_NONE, no certificates from the server are required (or looked at if provided))
    Refer to : https://<tetration>/documentation/ui/lab/managed_datatap.html
    :param cert_dir: directory where the certificate files are stored
    :return: ssl context
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    ctx.load_cert_chain(
        "{}{}".format(cert_folder, KAFKA_CONSUMER_CA),
        keyfile="{}{}".format(cert_folder, KAFKA_PRIVATE_KEY),
        password=None
    )
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def process_filters(processed_inventory_filters, filters, members_as_cidr, return_ip_set=False):
    for item in filters:
        print(item.name)
        processed_inventory_filters[item.id] = {
            'id': item.id, 'name': item.name, 'addresses': process_addresses(item.inventory_items, members_as_cidr)}


def process_scopes(processed_scopes, scopes):
    for item in scopes:
        processed_scopes[scopes[item].id] = {
            'id': scopes[item].id, 'name': scopes[item].name, 'catch_all': tnp_pb2.ScopeInfo.Action.Name(scopes[item].catch_all_action)}


def process_intents(processed_intents, intents, scopes, inventory_filters):
    for item in intents:
        intent = {'id': item.id,
                  'scope_id': item.meta_data.scope_id,
                  'scope_name': scopes[item.meta_data.scope_id]['name'],
                  'consumer_filter_id': item.flow_filter.consumer_filter_id,
                #   'consumer_filter_name': inventory_filters[item.flow_filter.consumer_filter_id]['name'],
                  'provider_filter_id': item.flow_filter.provider_filter_id,
                #   'provider_filter_name': inventory_filters[item.flow_filter.provider_filter_id]['name'],
                  'action': tnp_pb2.Intent.Action.Name(item.action),
                  'protocol': tnp_pb2.IPProtocol.Name(item.flow_filter.protocol_and_ports[0].protocol)
                  }
        try:
            intent['port_range'] = {'start_port': item.flow_filter.protocol_and_ports[0].port_ranges[0].start_port,
                                    'end_port': item.flow_filter.protocol_and_ports[0].port_ranges[0].end_port}
        except:
            pass
        processed_intents.append(intent)


def process_addresses(addresses, members_as_cidr, return_ip_set=True):
    processed_addresses = {}
    if return_ip_set:
        ip_set = netaddr.IPSet()
    for address in addresses:
        if address.ip_address.ip_addr != b'':
            address = address.ip_address
            if address.addr_family == 1:
                if 'subnets' not in processed_addresses:
                    processed_addresses['subnets']=[]
                processed_addresses['subnets'].append(str(ipaddress.ip_network(
                    '{}/{}'.format('.'.join(str(x) for x in address.ip_addr), address.prefix_length))))
                if return_ip_set:
                    ip_set.add(str(ipaddress.ip_network(
                        '{}/{}'.format('.'.join(str(x) for x in address.ip_addr), address.prefix_length))))
            elif address.addr_family == 2:
                if 'subnets' not in processed_addresses:
                    processed_addresses['subnets']=[]
                processed_addresses['subnets'].append(
                    str(ipaddress.ip_network(address.ip_addr)))
                if return_ip_set:
                    ip_set.add(str(ipaddress.ip_network(address.ip_addr)))
        elif address.address_range != b'':
            address = address.address_range
            if address.start_ip_addr == address.end_ip_addr:
                if 'ips' not in processed_addresses:
                    processed_addresses['ips']=[]
                ip = str(ipaddress.ip_address(address.start_ip_addr))
                processed_addresses['ips'].append(ip)
                if return_ip_set:
                    ip_set.add(ip)
            else:
                if members_as_cidr:
                    if 'subnets' not in processed_addresses:
                        processed_addresses['subnets']=[]
                    processed_addresses['subnets'] = processed_addresses['subnets'] + range_to_subnets(start_addr=str(ipaddress.ip_address(address.start_ip_addr)),end_addr=str(ipaddress.ip_address(address.end_ip_addr)))
                else:
                    if 'ranges' not in processed_addresses:
                        processed_addresses['ranges']=[]
                    ip_range={'start':str(ipaddress.ip_address(address.start_ip_addr)),'end':str(ipaddress.ip_address(address.end_ip_addr))}
                    processed_addresses['ranges'].append(ip_range)
                if return_ip_set:
                    ip_set.add(netaddr.IPRange(str(ipaddress.ip_address(address.start_ip_addr)),str(ipaddress.ip_address(address.end_ip_addr))))
        elif address.lb_service != b'':
            print('Im a load-balancer')
        else:
            print('Failed to process address.')
    if return_ip_set:
        print(ip_set)
    return processed_addresses

def range_to_subnets(start_addr, end_addr):
    return [str(x) for x in IPRange(start_addr,end_addr).cidrs()]

def dump_policy(policy, output_objects, json_out, yaml_out, save_to_file):
    output = {'version':policy.version,'create_timestamp':policy.create_timestamp}
    if 'SCOPES' in output_objects:
        output['scopes'] = policy.scopes
    if 'POLICIES' in output_objects:
        output['policies'] = policy.intents
    if 'FILTERS' in output_objects:
        output['filters'] = policy.inventory_filters

    if yaml_out:
        if save_to_file != None:
            with open('{}.yaml'.format(save_to_file), 'w') as outfile:
                yaml.dump(output,outfile)
            print('Saved Policy Update - Version: {}, Timestamp: {}, Filename: {}\n'.format(policy.version,policy.create_timestamp,'{}.yaml'.format(save_to_file)))
        else:
            print(yaml.dump(output))
    if json_out:
        if save_to_file != None:
            with open('{}.json'.format(save_to_file), 'w') as outfile:
                json.dump(output,outfile)
            print('Saved Policy Update - Version: {}, Timestamp: {}, Filename: {}\n'.format(policy.version,policy.create_timestamp,'{}.json'.format(save_to_file)))
        else:
            print(json.dumps(output,indent=1))


def create_consumer(policy,cert_folder):
    """
    Refer to Python package kafka-python, a high-level message consumer of Kafka brokers.
    The consumer iterator returns consumer records, which expose basic message
    attributes: topic, partition, offset, key, and value.
    :param args: Input arguments
    :param policy: Object to store Network Policy for processing
    :return: KafkaConsumer object, messages from the message bus for processing
    """

    f = open(cert_folder+"topic.txt", "r")
    kafka_topic = f.read()
    f.close()
    f = open(cert_folder+"kafkaBrokerIps.txt", "r")
    kafka_broker_ips = f.read()
    f.close()
    f = open(cert_folder+"consumer_name.txt", "r")
    client_id = f.read()
    f.close()

    consumer = KafkaConsumer(kafka_topic,
                             api_version=API_VERSION,
                             bootstrap_servers=kafka_broker_ips,
                             # name passed to servers for identification
                             client_id=client_id,
                             auto_offset_reset="latest",    # consume earliest or latest available msgs
                             enable_auto_commit=AUTOCOMMIT,             # autocommit offsets?
                             consumer_timeout_ms=65535,   # StopIteration if no message after 'n' seconds
                             max_partition_fetch_bytes=1048576,
                             security_protocol=SSL,
                             ssl_context=create_ssl_context(cert_folder)
                             )

    msg = ["All the topics available :{}".format(consumer.topics()),
           "Subscription:{}".format(consumer.subscription()),
           "Partitions for topic:{}".format(
        consumer.partitions_for_topic(kafka_topic)),
        "TopicPartitions:{}".format(consumer.assignment())
    ]
    print(msg)

    return consumer


def seek_to_latest_update_start(consumer):
    """
    The Tetration Kafka policy feed sends updates whenever there is an update to the policy. Updates can be trigged due to a variety of events.
    In most circumstances, the events will cause updates in a timely fashion within a 60 second timeout window.  That being said, in some more static
    environments, an update may not be recieved in time.  This function reads backwards on the message bus to identify and set the offset to the most recent UPDATE_START
    """
    seek_interval = 4
    found_start = False
    last_start_offset = 0
    end_offset = 0
    latest_seek = 0
    partition = None
    assignments = consumer.end_offsets(consumer.assignment())

    for partition in assignments:
        partition = partition
        end_offset = assignments[partition]
        latest_seek = end_offset

    protobuf = tnp_pb2.KafkaUpdate()

    while found_start == False and latest_seek - seek_interval > 0:
        latest_seek = latest_seek - seek_interval
        consumer.seek(partition, latest_seek)

        for count, message in enumerate(consumer):
            if message.offset == latest_seek+seek_interval-1:
                break
            # print("count:%d message_offset:%d len(value):%s" %
                #   (count, message.offset, len(message.value)))
            protobuf.ParseFromString(message.value)
            if protobuf.type == protobuf.UPDATE_START:
                found_start = True
                last_start_offset = message.offset
                # print("Found UPDATE_START at message offset:{}".format(
                #     message.offset))

    print('Setting offset to {} to process most recent policy update.'.format(
        last_start_offset))

    consumer.seek(partition, last_start_offset)

    return


def get_policy_update(policy, consumer):
    """
        Refer to the documentation at: https://<tetration>/documentation/ui/adm/policies.html
        for information on how network policy messages are published.
        Kafka messages are comprised of a topic, partition, offset, key and value.
        The key (message.key), for all records, have a value of 2. The message value
        (message.value) is a string which we load into the protocol buffer for decoding.
        We need to determine the field 'type' in the protocol buffer, to determine if it is an
        UPDATE, UPDATE_START, or UPDATE_END. End records (UPDATE_END) have a length of 8 bytes,
          e.g.  if len(message.value) == 8:
        and contain no data. Start (UPDATE_START) message contain data. Have not observed
        UPDATE records in testing, raise a ValueError exception to flag for future development.
        :param policy: Object to store Network Policy for processing
        :param input_data: messages from the Kafka Broder
        :return:
    """
    found_start = False

    # Work area for decoding the protocol buffer type.
    tmp_pbuf = tnp_pb2.KafkaUpdate()
    protobuf = tnp_pb2.KafkaUpdate()
    print('Waiting for policy update...\n')

    for count, message in enumerate(consumer):

        # Load the message value into the protocol buffer
        tmp_pbuf.ParseFromString(message.value)

        if tmp_pbuf.type > 2:
            # Any types other than 0,1,2 are unexpected
            raise ValueError("Unknown type:{} at message offset:{}".format(
                protobuf.type, message.offset))

        if tmp_pbuf.type == protobuf.UPDATE and found_start:
            protobuf.MergeFromString(message.value)
            # Replace what was saved when we found_start
            policy.buffer = protobuf
            raise ValueError(
                "Encountered UPDATE record at message offset:{}, logic not tested".format(message.offset))
            # continue   TODO Once tested, you should remove the exception and continue

        if tmp_pbuf.type == protobuf.UPDATE_END and found_start:
            policy.update_end_offset = message.offset
            break

        if tmp_pbuf.type == protobuf.UPDATE_START:
            found_start = True
            # Load the message value into the protocol buffer
            protobuf.ParseFromString(message.value)

        if found_start:
            policy.buffer = protobuf
            continue

        else:
            print("Skipping message offset:{}".format(message.offset))
            continue


def main():
    """
    Main Logic
    """
    
    parser = argparse.ArgumentParser(
        description='Secure Workload - Kafka Policy Client')
    parser.add_argument('--cert_folder', help='Kafka Certificate Folder', type=str, required=True)
    parser.add_argument('--scopes', action='store_true', default=False,
                        help='Include scopes in output.')
    parser.add_argument('--filters', action='store_true', default=False,
                        help='Include filters in output.')
    parser.add_argument('--members_as_cidr', action='store_true', default=False,
                        help='Changes the output of filter membership from the default behavior of IP Ranges to a set of summarized subnets.')
    parser.add_argument('--policies', action='store_true', default=False,
                        help='Include policies in output.')
    parser.add_argument('--loop', action='store_true', default=False,
                        help='Default behavior is to download a single policy update and exit.  This flag changes the behaviour to continuously listen for policy updates.')
    parser.add_argument('--seek_to_last', action='store_true', default=False,
                        help='Certain scenarios can pause policy updates, which can cause the script to hang.  This flag seeks back in the policy stream to retrieve the most recent update vs. waiting for the next.')
    parser.add_argument('--json', action='store_true', default=False,
                        help='Set the output format to JSON.')
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Set the output format to YAML.')
    parser.add_argument('--save_to_file', type=str,
                        help='Filename to save without extension.')
    args = parser.parse_args()

    output_objects = []
    if args.scopes:
        output_objects.append('SCOPES')
    if args.filters:
        output_objects.append('FILTERS')
    if args.policies:
        output_objects.append('POLICIES')
    if len(output_objects) == 0:
        print('ERROR: No output objects selected. Please select at least 1 output object with a command line argument.')
        exit()
    
    if args.json==False and args.yaml==False:
        print('ERROR: No output format selected. Please select either JSON or YAML as the output format.')
        exit()

    policy = PolicySet()
    consumer = create_consumer(policy, args.cert_folder)
    if args.seek_to_last:
        seek_to_latest_update_start(consumer)

    while True:
        get_policy_update(policy, consumer)
        if policy.buffer:
            policy.version= policy.buffer.version
            policy.create_timestamp = policy.buffer.create_timestamp
            if 'SCOPES' in output_objects or 'POLICIES' in output_objects:
                process_scopes(
                    policy.scopes, policy.buffer.tenant_network_policy.scopes)
            for item in policy.buffer.tenant_network_policy.network_policy:
                process_filters(policy.inventory_filters,
                                item.inventory_filters, args.members_as_cidr)
                if 'POLICIES' in output_objects:
                    process_intents(policy.intents, item.intents,
                                    policy.scopes, policy.inventory_filters)
        
        dump_policy(policy, output_objects, args.json, args.yaml, args.save_to_file)
        if args.loop == False:
            break


if __name__ == '__main__':
    main()
