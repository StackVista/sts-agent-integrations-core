# (C) StackState, Inc. 2019
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)
import json
import logging
import time

import cm_client
from checks import AgentCheck
from cm_client.rest import ApiException
from config import initialize_logging, _is_affirmative as is_affirmative


class ClouderaCheck(AgentCheck):
    INSTANCE_TYPE = 'cloudera'
    SERVICE_CHECK_NAME = 'cloudera.can_connect'
    EVENT_TYPE = 'cloudera.entity_status'
    EVENT_MESSAGE = '{} status'

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)
        self.url = None
        self.tags = None
        self.instance_key = None
        self.roles = None

    def check(self, instance):
        self.url = get_config(instance)[0]

        self.tags = ['instance_url: {}'.format(self.url)]
        self.instance_key = {'type': self.INSTANCE_TYPE, 'url': self.url}
        self.roles = []

        # collect topology
        self.start_snapshot(self.instance_key)
        try:
            api_client = ClouderaClient(instance)
            self._collect_topology(api_client)
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.OK, tags=self.tags)
        except ApiException as e:
            try:
                error_msg = json.loads(e.body)
                msg = 'Status: {} {} - Reason: {}'.format(e.status, e.reason, error_msg['message'])
            except ValueError:
                msg = 'Status: {} {}'.format(e.status, e.reason)
            self.log.error(msg)
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL, message=msg, tags=self.tags)
        except Exception as e:
            msg = 'Cloudera check failed: {}'.format(e)
            self.log.error(msg)
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL, message=msg, tags=self.tags)
        finally:
            self.stop_snapshot(self.instance_key)

    def _collect_topology(self, api_client):
        self._collect_cluster(api_client)
        self._collect_hosts(api_client)

    def _collect_hosts(self, api_client):
        host_api_response = api_client.get_host_api()
        for host_data in host_api_response.items:
            data = self._dict_from_cls(host_data)
            hostname = host_data.hostname.split('.')[0]
            data['identifiers'] = ['urn:host:/{}'.format(hostname), host_data.host_id]
            self.component(self.instance_key, hostname, {'name': 'host'}, data)
            self.event(self._create_event_data(hostname, host_data.entity_status))
            for role in host_data.role_refs:
                if role.role_name in self.roles:
                    self.relation(self.instance_key, role.role_name, hostname, {'name': 'is hosted on'}, {})

    def _collect_cluster(self, api_client):
        cluster_api_response = api_client.get_cluster_api()
        for cluster_data in cluster_api_response.items:
            data = self._dict_from_cls(cluster_data)
            data['name'] = cluster_data.display_name
            data['identifiers'] = ['urn:clouderacluster:/{}'.format(cluster_data.name)]
            self.component(self.instance_key, cluster_data.name, {'name': 'cluster'}, data)
            self.event(self._create_event_data(cluster_data.name, cluster_data.entity_status))
            self._collect_services(api_client, cluster_data.name)

    def _collect_services(self, api_client, cluster_name):
        service_api_response = api_client.get_service_api(cluster_name)
        for service_data in service_api_response.items:
            self.component(self.instance_key, service_data.name, {'name': 'service'}, self._dict_from_cls(service_data))
            self.event(self._create_event_data(service_data.name, service_data.entity_status))
            self.relation(self.instance_key, cluster_name, service_data.name, {'name': 'runs on'}, {})
            self._collect_roles(api_client, cluster_name, service_data.name)

    def _collect_roles(self, api_client, cluster_name, service_name):
        roles_api_response = api_client.get_roles_api(cluster_name, service_name)
        for role_data in roles_api_response.items:
            self.component(self.instance_key, role_data.name, {'name': 'role'}, self._dict_from_cls(role_data))
            self.event(self._create_event_data(role_data.name, role_data.entity_status))
            self.relation(self.instance_key, service_name, role_data.name, {'name': 'executes'}, {})
            self.roles.append(role_data.name)

    def _dict_from_cls(self, cls):
        data = dict((key.lstrip('_'), str(value)) for (key, value) in cls.__dict__.items())
        data.update({'cloudera-instance': self.url})
        return data

    def _create_event_data(self, name, status):
        return {
            'timestamp': int(time.time()),
            'source_type_name': self.EVENT_TYPE,
            'msg_title': self.EVENT_MESSAGE.format(name),
            'host': name,
            'msg_text': status,
            'tags': self.tags + ['entity_name: {}'.format(name), 'type: {}'.format(self.EVENT_TYPE)]
        }


class ClouderaClient:
    def __init__(self, instance):
        self.log = logging.getLogger(__name__)
        self.url, user, password, api_version, verify_ssl = get_config(instance)

        if not user:
            raise Exception('Cloudera Manager user name is required.')

        if not password:
            raise Exception('Cloudera Manager user password is required.')

        # Configure HTTP basic authorization: basic
        cm_client.configuration.username = user
        cm_client.configuration.password = password
        cm_client.configuration.verify_ssl = verify_ssl

        # Construct base URL for API
        api_url = '{0}/api/{1}'.format(self.url, api_version)

        self.api_client = cm_client.ApiClient(api_url)

    def get_cluster_api(self):
        try:
            cluster_api_instance = cm_client.ClustersResourceApi(self.api_client)
            cluster_api_response = cluster_api_instance.read_clusters(view='full')
            return cluster_api_response
        except ApiException as e:
            self.log.error('ERROR with ClustersResourceApi > read_clusters at {}'.format(self.url))
            raise e

    def get_host_api(self):
        try:
            host_api_instance = cm_client.HostsResourceApi(self.api_client)
            host_api_response = host_api_instance.read_hosts(view='full')
            return host_api_response
        except ApiException as e:
            self.log.error('ERROR with ClustersResourceApi > read_hosts at {}'.format(self.url))
            raise e

    def get_service_api(self, cluster_name):
        try:
            services_api_instance = cm_client.ServicesResourceApi(self.api_client)
            services_api_response = services_api_instance.read_services(cluster_name, view='full')
            return services_api_response
        except ApiException as e:
            self.log.error('ERROR with ServicesResourceApi > read_services at {}'.format(self.url))
            raise e

    def get_roles_api(self, cluster_name, service_name):
        try:
            roles_api_instance = cm_client.RolesResourceApi(self.api_client)
            roles_api_response = roles_api_instance.read_roles(cluster_name, service_name, view='full')
            return roles_api_response
        except ApiException as e:
            self.log.error('ERROR with RolesResourceApi > read_roles at {}'.format(self.url))
            raise e


def get_config(instance):
    url = instance.get('url', '')
    api_version = instance.get('api_version', '')
    user = instance.get('username', '')
    password = str(instance.get('password', ''))
    verify_ssl = is_affirmative(instance.get('verify_ssl'))
    return url, user, password, api_version, verify_ssl
