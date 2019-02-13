"""
    StackState.
    Zabbix telemetry integration
"""

import requests
import logging
import time

from checks import AgentCheck, CheckException
from utils.persistable_store import PersistableStore


class ZabbixHost:
    def __init__(self, hostid, host, name):
        self.host_id = hostid
        self.host = host
        self.name = name


class ZabbixTrigger:
    def __init__(self, trigger_id, description, priority):
        self.trigger_id = trigger_id
        self.description = description
        self.priority = priority # translates to severity


class ZabbixEvent:
    def __init__(self, event_id, value, acknowledged, hosts, trigger):
        self.event_id = event_id
        self.acknowledged = acknowledged # 0/1 (not) acknowledged
        self.value = value # 0/1 -> inactive/active
        self.hosts = hosts # ZabbixHost list
        self.trigger = trigger # ZabbixTrigger

class ZabbixProblem:
    def __init__(self, event_id, host, trigger_name, acknowledged, trigger_id, severity):
        self.event_id = event_id
        self.host = host
        self.trigger_name = trigger_name
        self.acknowledged = acknowledged
        self.trigger_id = trigger_id
        self.severity = severity

class ZabbixHostStates:
    _states = {} # host_id: {trigger_id -> [ZabbixEvent]}

    def update(self, zabbix_event):
        for host in zabbix_event.hosts:
            host_id = host.host_id
            trigger_id = zabbix_event.trigger.trigger_id
            self._states[host_id][trigger_id].append(zabbix_event)

    def get_most_severe_zabbix_event(self, host_id):
        most_severe_zabbix_severity = 0
        most_severe_zabbix_event = None
        for trigger_id, zabbix_events in self._states[host_id].iteritems():
            for zabbix_event in zabbix_events:
                if zabbix_event.trigger.priority > most_severe_zabbix_severity:
                    most_severe_zabbix_event = zabbix_event
        # TODO take ACKs into account
        return most_severe_zabbix_event



class Zabbix(AgentCheck):
    SERVICE_CHECK_NAME = SOURCE_TYPE_NAME = "Zabbix"
    log = logging.getLogger('Zabbix')
    begin_epoch = None # start to listen to events from epoch timestamp
    host_states = ZabbixHostStates()

    def check(self, instance):
        """
        Integration logic
        """
        if 'url' not in instance:
            raise CheckException('Missing API url in configuration.')
        if 'user' not in instance:
            raise CheckException('Missing user in configuration.')
        if 'password' not in instance:
            raise CheckException('Missing password in configuration.')

        url = instance['url']

        topology_instance = {
            "type": self.SERVICE_CHECK_NAME,
            "url": url
        }

        self.check_connection(url)
        auth = self.login(url, instance['user'], instance['password'])

        self.start_snapshot(topology_instance)

        for zabbix_host in self.retrieve_hosts(url, auth):
            # TODO host_group as domain
            # TODO environment configurable in conf yaml
            self.process_host_topology(topology_instance, zabbix_host)

            zabbix_problems = self.retrieve_problems(url, auth, [zabbix_host.host_id]) # TODO combine host ids in one payload, batching
            self.process_problems(zabbix_host, zabbix_problems)

        self.stop_snapshot(topology_instance)

    def process_problems(self, zabbix_host, zabbix_problems):
        most_severe = None
        severe_problems = []
        for zabbix_problem in zabbix_problems:
            if not most_severe:
                most_severe = zabbix_problem.severity
                severe_problems.append(zabbix_problem)
            elif zabbix_problem.severity == most_severe:
                severe_problems.append(zabbix_problem)
            elif zabbix_problem.severity > most_severe:
                most_severe = zabbix_problem.severity
                severe_problems = [zabbix_problem]

        # TODO what if all highs/disasters are acked!

        # TODO is there an ACK in severe_problems?


        self.event({
            'timestamp': int(time.time()),
            'source_type_name': self.SOURCE_TYPE_NAME,
            'host': self.hostname,
            'tags': [
                'host_name:%s' % zabbix_host.name,
                'host_id:%s' % zabbix_host.host_id,
                'host:%s' % zabbix_host.host,
                'severity:%s' % 0  # TODO
                # TODO problems
            ]
        })


        print("")

    def process_host_health_state(self, zabbix_event):
        self.host_states.update(zabbix_event)
        for host in zabbix_event.hosts:
            zabbix_event = self.host_states.get_most_severe_zabbix_event(host.host_id)

            triggers = "triggers:[%s]" % ','.join(zabbix_event)

            self.event({
                'timestamp': int(time.time()),
                'source_type_name': self.SOURCE_TYPE_NAME,
                'host': self.hostname,
                'tags': [
                    'host:%s' % host,
                    'host_id:%s' % "",
                    triggers
                ]
            })


    def process_host_topology(self, topology_instance, zabbix_host):
        external_id = zabbix_host.host
        data = {
            'name': zabbix_host.name,
            'host_id': zabbix_host.host_id,
            'host': zabbix_host.host,
            'layer': 'Host',
            'domain': 'Zabbix', # TODO host group (+filter_
            'environment': 'Production' # TODO make configurable in yaml
        }
        component_type = {"name": "zabbix_host"}

        self.component(topology_instance, external_id, component_type, data=data)

    def parse_event(self, event):
        event_id = event.get('event', None)
        value = event.get('value', None)
        acknowledged = event.get('acknowledged', None)

        hosts_items = event.get('hosts', [])
        hosts = []
        for item in hosts_items:
            host_id = item.get('hostid', None)
            host = item.get('host', None)
            host_name = item.get('name', None)
            zabbix_host = ZabbixHost(host_id, host, host_name)
            hosts.append(zabbix_host)

        trigger = event.get('relatedObject', {})
        trigger_id = trigger.get('triggerid', None)
        trigger_description = trigger.get('description', None)
        trigger_priority = trigger.get('priority', None)

        trigger = ZabbixTrigger(trigger_id, trigger_description, trigger_priority)
        zabbix_event = ZabbixEvent(event_id, value, acknowledged, hosts, trigger)
        self.log.debug("Parsed ZabbixEvent: %s." % zabbix_event)

        # TODO check for None values and give self.log.warn()
        return zabbix_event

    def retrieve_events(self, url, auth, begin_epoch, end_epoch):
        params = {
            "object": 0, # trigger events
            "output": ["eventid", "value", "acknowledged"],
            "time_from": begin_epoch,
            "time_till": end_epoch,
            "selectHosts": ["hostid", "host"],
            "selectRelatedObject": ["triggerid", "description", "priority"],
            "sortfield": ["clock", "eventid"],
            "sortorder": "ASC"
        }

        response = self.method_request(url, "event.get", auth=auth, params=params)
        return iter(response['result'])

    # TODO pagination
    def retrieve_hosts(self, url, auth):
        self.log.debug("Retrieving hosts.")
        params = {
            "output": ["hostid", "host", "name"]
        }
        response = self.method_request(url, "host.get", auth=auth, params=params)
        for item in response.get("result", []):
            host_id = item.get("hostid", None)
            host = item.get("host", None)
            name = item.get("name", None)
            yield ZabbixHost(host_id, host, name)


    def retrieve_problems(self, url, auth, host_ids):
        assert(type(host_ids) == list)

        params = {
            "hostids": host_ids, # filter on specific host ids
            "object": 0, # only interested in triggers
            "output": ["hostid", "host", "name"]
        }
        response = self.method_request(url, "problem.get", auth=auth, params=params)
        for item in response.get('result', []):
            event_id = item.get("eventid", None)
            host = item.get("host", None)
            trigger_name = item.get("name", None)
            acknowledged = item.get("acknowledged", None)
            trigger_id = item.get("objectid", None) # Object id is in case of object=0 a trigger.
            severity = item.get("severity", self.get_trigger_priority(url, auth, trigger_id)) # for Zabbix versions <4.0 we need to get the trigger.priority

            zabbix_problem = ZabbixProblem(event_id, host, trigger_name, acknowledged, trigger_id, severity)
            self.log.debug("Parsed ZabbixProblem %s." % zabbix_problem)
            yield  zabbix_problem


    def get_trigger_priority(self, url, auth, trigger_id):
        params = {
            "output": ["priority"],
            "triggerids": trigger_id
        }
        response = self.method_request(url, "trigger.get", auth=auth, params=params)
        trigger = response.get('result', [None])[0] # get first element or None
        return trigger.get("priority", None)


    def check_connection(self, url):
        """
        Check Zabbix connection
        :param url: Zabbix API location
        :return: None
        """
        self.log.debug("Checking connection.")
        try:
            response = self.method_request(url, "apiinfo.version")
            version = response['result']
            self.log.info("Connected to Zabbix version %s." % version)
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.OK)
        except Exception as e:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL, message="Failed to connect to Zabbix on url %s. Please refer to Agent's collector.log log file." % url)
            raise e

    def login(self, url, user, password):
        """
        Log in into Zabbix with provided credentials
        :param url: Zabbix API location
        :param user: in configuration provided credentials
        :param password: in configuration provided credentials
        :return: session string to use in subsequent requests
        """
        self.log.debug("Logging in.")
        params = {
            "user": user,
            "password": password
        }
        try:
            response = self.method_request(url, "user.login", params=params)
            return response['result']
        except Exception as e:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL, message="Failed to log in into Zabbix with provided credentials." % url)
            raise e

    def method_request(self, url, name, auth=None, params={}, request_id=1):
        payload = {
            "jsonrpc": "2.0",
            "method": "%s" % name,
            "id": request_id,
            "params": params
        }
        if auth:
            payload['auth'] = auth

        self.log.debug("Request payload: %s" % payload)
        response = requests.get(url, json=payload)
        response.raise_for_status()
        self.log.debug("Request response: %s" % response.text)
        return response.json()


