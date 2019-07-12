"""
    StackState.
    Zabbix host topology and problem integration

    Current limitations:
    - Unsupported Zabbix Problem acks
    - Unsupported Zabbix version 4+ severity upgrades, currently the trigger's severity is used for compatibility
"""

import requests
import logging
import time
import re

from checks import AgentCheck, CheckException


class ZabbixHost:
    def __init__(self, host_id, host, name, host_groups):
        assert(type(host_groups == list))
        self.host_id = host_id
        self.host = host
        self.name = name
        self.host_groups = host_groups

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "ZabbixHost(host_id:%s, host:%s, name:%s, host_groups:%s.)" % (self.host_id, self.host, self.name, self.host_groups)


class ZabbixHostGroup:
    def __init__(self, host_group_id, name):
        self.host_group_id = host_group_id
        self.name = name

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "ZabbixHostGroup(host_group_id:%s, name:%s.)" % (self.host_group_id, self.name)


class ZabbixTrigger:
    def __init__(self, trigger_id, description, priority):
        self.trigger_id = trigger_id
        self.description = description
        self.priority = priority  # translates to severity

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "ZabbixTrigger(trigger_id:%s, description:%s, priority:%s.)" % (self.trigger_id, self.description, self.priority)


class ZabbixEvent:
    def __init__(self, event_id, acknowledged, host_ids, trigger):
        self.event_id = event_id
        self.acknowledged = acknowledged  # 0/1 (not) acknowledged
        self.host_ids = host_ids
        self.trigger = trigger  # ZabbixTrigger

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "ZabbixEvent(event_id:%s, acknowledged:%s, host_ids:%s, trigger:%s)" % (self.event_id, self.acknowledged, self.host_ids, self.trigger)


class ZabbixProblem:
    def __init__(self, event_id, acknowledged, trigger_id, severity):
        self.event_id = event_id
        self.acknowledged = acknowledged
        self.trigger_id = trigger_id
        self.severity = severity

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "ZabbixProblem(event_id:%s, acknowledged:%s, trigger_id:%s, severity:%s)" % (self.event_id, self.acknowledged, self.trigger_id, self.severity)


class Zabbix(AgentCheck):
    SERVICE_CHECK_NAME = SOURCE_TYPE_NAME = "Zabbix"
    log = logging.getLogger('Zabbix')
    begin_epoch = None  # start to listen to events from epoch timestamp

    def check(self, instance):
        """
        Integration logic
        """
        if 'url' not in instance:
            raise CheckException('Missing API url in configuration.')
        if 'user' not in instance:
            raise CheckException('Missing API user in configuration.')
        if 'password' not in instance:
            raise CheckException('Missing API password in configuration.')

        stackstate_environment = instance.get('stackstate_environment', 'Production')

        url = instance['url']

        topology_instance = {
            "type": self.SERVICE_CHECK_NAME,
            "url": url
        }

        self.check_connection(url)
        auth = self.login(url, instance['user'], instance['password'])

        self.start_snapshot(topology_instance)

        hosts = {}  # key: host_id, value: ZabbixHost

        # Topology, get all hosts
        for zabbix_host in self.retrieve_hosts(url, auth):
            self.process_host_topology(topology_instance, zabbix_host, stackstate_environment)

            hosts[zabbix_host.host_id] = zabbix_host

        # Telemetry, get all problems.
        zabbix_problems = self.retrieve_problems(url, auth)

        event_ids = list(problem.event_id for problem in zabbix_problems)
        zabbix_events = [] if len(event_ids) == 0 else self.retrieve_events(url, auth, event_ids)

        rolled_up_events_per_host = {}  # host_id -> [ZabbixEvent]
        most_severe_severity_per_host = {}  # host_id -> severity int
        for zabbix_event in zabbix_events:
            for host_id in zabbix_event.host_ids:
                if host_id in rolled_up_events_per_host:
                    rolled_up_events_per_host[host_id].append(zabbix_event)
                    if most_severe_severity_per_host[host_id] < zabbix_event.trigger.priority:
                        most_severe_severity_per_host[host_id] = zabbix_event.trigger.priority
                else:
                    rolled_up_events_per_host[host_id] = [zabbix_event]
                    most_severe_severity_per_host[host_id] = zabbix_event.trigger.priority

        self.log.debug('rolled_up_events_per_host:' + str(rolled_up_events_per_host))
        self.log.debug('most_severe_severity_per_host:' + str(most_severe_severity_per_host))

        # iterate all hosts to send an event per host, either in OK/PROBLEM state
        for host_id, zabbix_host in hosts.iteritems():
            severity = 0
            triggers = []

            if host_id in rolled_up_events_per_host:
                triggers = [event.trigger.description for event in rolled_up_events_per_host[host_id]]
                severity = most_severe_severity_per_host[host_id]

            self.event({
                'timestamp': int(time.time()),
                'source_type_name': self.SOURCE_TYPE_NAME,
                'host': self.hostname,
                'tags': [
                    'host_id:%s' % host_id,
                    'host:%s' % zabbix_host.host,
                    'host_name:%s' % zabbix_host.name,
                    'severity:%s' % severity,
                    'triggers:%s' % triggers
                ]
            })

        self.stop_snapshot(topology_instance)

    def process_host_topology(self, topology_instance, zabbix_host, stackstate_environment):
        external_id = "urn:host:/%s" % zabbix_host.host
        url = topology_instance.get('url')
        instance_url = re.search(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", url).group(0)
        labels = ['zabbix', 'instance_url:%s' % instance_url]
        for host_group in zabbix_host.host_groups:
            labels.append('host group:%s' % host_group.name)
        data = {
            'name': zabbix_host.name,
            'host_id': zabbix_host.host_id,
            'host': zabbix_host.host,
            'layer': 'Host',
            'domain': zabbix_host.host_groups[0].name if len(zabbix_host.host_groups) == 1 else 'Zabbix',  # use host group of component as StackState domain when there is only one host group
            'identifiers': [zabbix_host.host],
            'environment': stackstate_environment,
            'host_groups': [host_group.name for host_group in zabbix_host.host_groups],
            'labels': labels,
            'instance': instance_url
        }
        component_type = {"name": "zabbix_host"}

        self.component(topology_instance, external_id, component_type, data=data)

    def retrieve_events(self, url, auth, event_ids):
        assert(type(event_ids) == list)
        self.log.debug("Retrieving events for event_ids: %s." % event_ids)

        params = {
            "object": 0,  # trigger events
            "eventids": event_ids,
            "output": ["eventid", "value", "severity", "acknowledged"],
            "selectHosts": ["hostid"],
            "selectRelatedObject": ["triggerid", "description", "priority"]
        }

        response = self.method_request(url, "event.get", auth=auth, params=params)

        events = response.get('result', [])
        for event in events:
            event_id = event.get('eventid', None)
            acknowledged = event.get('acknowledged', None)

            hosts_items = event.get('hosts', [])
            host_ids = []
            for item in hosts_items:
                host_id = item.get('hostid', None)
                host_ids.append(host_id)

            trigger = event.get('relatedObject', {})
            trigger_id = trigger.get('triggerid', None)
            trigger_description = trigger.get('description', None)
            trigger_priority = trigger.get('priority', None)

            trigger = ZabbixTrigger(trigger_id, trigger_description, trigger_priority)
            zabbix_event = ZabbixEvent(event_id, acknowledged, host_ids, trigger)

            self.log.debug("Parsed ZabbixEvent: %s." % zabbix_event)

            if not trigger_id or not trigger_description or not trigger_priority or not event_id or len(host_ids) == 0:
                self.log.warn("Incomplete ZabbixEvent, got: %s" % zabbix_event)

            yield zabbix_event

    def retrieve_hosts(self, url, auth):
        self.log.debug("Retrieving hosts.")
        params = {
            "output": ["hostid", "host", "name"],
            "selectGroups": ["groupid", "name"]
        }
        response = self.method_request(url, "host.get", auth=auth, params=params)
        for item in response.get("result", []):
            host_id = item.get("hostid", None)
            host = item.get("host", None)
            name = item.get("name", None)
            raw_groups = item.get('groups', [])
            groups = []
            for raw_group in raw_groups:
                host_group_id = raw_group.get('groupid', None)
                host_group_name = raw_group.get('name', None)
                zabbix_host_group = ZabbixHostGroup(host_group_id, host_group_name)
                groups.append(zabbix_host_group)

            zabbix_host = ZabbixHost(host_id, host, name, groups)
            if not host_id or not host or not name or len(groups) == 0:
                self.log.warn("Incomplete ZabbixHost, got: %s" % zabbix_host)

            yield zabbix_host

    def retrieve_problems(self, url, auth):
        self.log.debug("Retrieving problems.")

        params = {
            "object": 0,  # only interested in triggers
            "output": ["severity", "objectid", "acknowledged"]
        }
        response = self.method_request(url, "problem.get", auth=auth, params=params)
        for item in response.get('result', []):
            event_id = item.get("eventid", None)
            acknowledged = item.get("acknowledged", None)
            trigger_id = item.get("objectid", None)  # Object id is in case of object=0 a trigger.
            severity = item.get("severity", self.get_trigger_priority(url, auth, trigger_id)) # for Zabbix versions <4.0 we need to get the trigger.priority

            zabbix_problem = ZabbixProblem(event_id, acknowledged, trigger_id, severity)

            self.log.debug("Parsed ZabbixProblem %s." % zabbix_problem)
            if not event_id or not trigger_id or not severity:
                self.log.warn("Incomplete ZabbixProblem, got: %s" % zabbix_problem)

            yield zabbix_problem

    def get_trigger_priority(self, url, auth, trigger_id):
        params = {
            "output": ["priority"],
            "triggerids": [trigger_id]
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
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL, message="Failed to log in into Zabbix at %s with provided credentials." % url)
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
