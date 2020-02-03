import os
import jsonpickle as jsonpickle
from config import initialize_logging
from tests.checks.common import AgentCheckTest, load_check

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse


class MockClouderaClient:
    def __init__(self, instance):
        pass

    def get_cluster_api(self):
        return self.read_data(self.get_file('cluster_api_response.json'))

    def get_host_api(self):
        return self.read_data(self.get_file('host_api_response.json'))

    def get_service_api(self, cluster_name):
        return self.read_data(self.get_file('services_api_response_{}.json'.format(cluster_name)))

    def get_roles_api(self, cluster_name, service_name):
        return self.read_data(self.get_file('roles_api_response_{}_{}.json'.format(cluster_name, service_name)))

    @staticmethod
    def read_data(file_name):
        with open(file_name, 'r') as file:
            json_file = file.read()
        return jsonpickle.decode(json_file)

    @staticmethod
    def get_file(file_name):
        return os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data', file_name)


class TestCloudera(AgentCheckTest):
    CHECK_NAME = 'cloudera'
    initialize_logging(CHECK_NAME)

    def setUp(self):
        self.agent_config = {
            'version': '0.1',
            'api_key': 'API_KEY'
        }
        self.config = {
            'instances': [
                {
                    'url': 'https://localhost:8080',
                    'username': 'admin',
                    'password': 'secret',
                    'api_version': 'v16',
                    'ssl_validation': False,
                },
            ]
        }

    @patch('check.ClouderaClient', MockClouderaClient)
    def test_check_collect_topology(self):
        self.run_check(self.config)
        topologies = self.check.get_topology_instances()
        for topology in topologies:
            assert len(topology['components']) == 36
            assert len(topology['relations']) == 57
