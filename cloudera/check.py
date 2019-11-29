# (C) Datadog, Inc. 2019
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)
import time
import cm_client
from cm_client.rest import ApiException
import json
from urlparse import urlparse

from checks import AgentCheck, _is_affirmative
from config import initialize_logging


class Cloudera(AgentCheck):
    INSTANCE_TYPE = 'cloudera'
    SERVICE_CHECK_NAME = 'cloudera.can_connect'
    EVENT_TYPE = 'cloudera.entity_status'
    EVENT_MESSAGE = '{} status'

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)
        self.url = None
        self.tags = None
        self.instance_key = None

    def check(self, instance):
        self.url, port, user, password, api_version, verify_ssl = self._get_config(instance)

        if not user:
            raise Exception('Cloudera Manager user name is required.')

        if not password:
            raise Exception('Cloudera Manager user password is required.')

        # Configure HTTP basic authorization: basic
        cm_client.configuration.username = user
        cm_client.configuration.password = password
        if verify_ssl:
            cm_client.configuration.verify_ssl = True

        # Construct base URL for API
        # TODO what to do when we have no port
        api_url = self.url + ':' + str(port) + '/api/' + api_version

        self.tags = ['instance_url: {}'.format(self.url)]

        self.instance_key = {'type': self.INSTANCE_TYPE, 'url': self.url}
        try:
            api_client = cm_client.ApiClient(api_url)

            # collect topology
            self.start_snapshot(self.instance_key)
            self._collect_topology(api_client)
            self.stop_snapshot(self.instance_key)

            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.OK, tags=self.tags)
        except ApiException as e:
            error_msg = json.loads(e.body)
            msg = 'Cloudera check {} failed: {}'.format(e.request_name, error_msg['message'])
            self.log.error(msg)
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL, message=msg, tags=self.tags)
        except Exception as e:
            msg = 'Cloudera check failed: {}'.format(str(e))
            self.log.error(msg)
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL, message=msg, tags=self.tags)

    def _collect_topology(self, api_client):
        self._collect_hosts(api_client)
        self._collect_cluster(api_client)

    def _collect_hosts(self, api_client):
        try:
            host_api_instance = cm_client.HostsResourceApi(api_client)
            host_api_response = host_api_instance.read_hosts(view='full')
            for host_data in host_api_response.items:
                self.component(self.instance_key, host_data.host_id, 'host', self._dict_from_cls(host_data))
                self.event(self._create_event_data(host_data.host_id, host_data.entity_status))
        except ApiException as e:
            e.request_name = 'ClustersResourceApi > read_hosts'
            raise e

    def _collect_cluster(self, api_client):
        try:
            cluster_api_instance = cm_client.ClustersResourceApi(api_client)
            cluster_api_response = cluster_api_instance.read_clusters(view='full')
            for cluster_data in cluster_api_response.items:
                self.component(self.instance_key, cluster_data.name, 'cluster', self._dict_from_cls(cluster_data))
                self.event(self._create_event_data(cluster_data.name, cluster_data.entity_status))
                hosts_api_response = cluster_api_instance.list_hosts(cluster_data.name)
                for host_data in hosts_api_response.items:
                    self.relation(self.instance_key, cluster_data.name, host_data.host_id, {'name': 'is hosted on'}, {})
                self._collect_services(api_client, cluster_data.name)
        except ApiException as e:
            e.request_name = 'ClustersResourceApi > read_clusters'
            raise e

    def _collect_services(self, api_client, cluster_name):
        try:
            services_api_instance = cm_client.ServicesResourceApi(api_client)
            resp = services_api_instance.read_services(cluster_name, view='full')
            for service_data in resp.items:
                self.component(self.instance_key, service_data.name, 'service', self._dict_from_cls(service_data))
                self.event(self._create_event_data(service_data.name, service_data.entity_status))
                self.relation(self.instance_key, service_data.name, cluster_name, {'name': 'runs on'}, {})
                self._collect_roles(api_client, cluster_name, service_data.name)
        except ApiException as e:
            e.request_name = 'ServicesResourceApi > read_services'
            raise e

    def _collect_roles(self, api_client, cluster_name, service_name):
        try:
            roles_api_instance = cm_client.RolesResourceApi(api_client)
            roles_api_response = roles_api_instance.read_roles(cluster_name, service_name, view='full')
            for role_data in roles_api_response.items:
                self.component(self.instance_key, role_data.name, 'role', self._dict_from_cls(role_data))
                self.event(self._create_event_data(role_data.name, role_data.entity_status))
                self.relation(self.instance_key, role_data.name, service_name, {'name': 'executes'}, {})
        except ApiException as e:
            e.request_name = 'RolesResourceApi > read_roles'
            raise e

    @staticmethod
    def _get_config(instance):
        url = instance.get('url', '')
        port = instance.get('port', 0)
        api_version = instance.get('api_version', '')
        user = instance.get('username', '')
        password = str(instance.get('password', ''))
        verify_ssl = _is_affirmative(instance.get('verify_ssl'))
        return url, port, user, password, api_version, verify_ssl

    def _dict_from_cls(self, cls):
        data = dict((key, str(value)) for (key, value) in cls.__dict__.items())
        data.update({'cloudera-instance': urlparse(self.url).netloc})
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


if __name__ == '__main__':
    initialize_logging('cloudera')
    check, instances = Cloudera.from_yaml('/Users/hruhek/docker/sts-cloudera/cloudera.yaml')
    for instance in instances:
        print "\nRunning the check against url: %s" % (instance['url'])
        check.check(instance)
        if check.has_events():
            print 'Events: %s' % (check.get_events())
        print 'Metrics: %s' % (check.get_metrics())
        print check.get_topology_instances()
