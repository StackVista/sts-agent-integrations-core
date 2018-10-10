# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
from nose.plugins.attrib import attr

# 3p
import mock
import json

# project
from tests.checks.common import AgentCheckTest
from checks import AgentCheck, CheckException


instance = {
    'url': "https://dev6047.service-now.com",
    'basic_auth': {'user': 'admin', 'password': 'Service@123'},
    'default_batch_size': 100
}

CONFIG = {
    'init_config': {'default_timeout': 10, 'min_collection_interval': 5},
    'instances': [
        {
            'url': "https://dev60479.service-now.com",
            'basic_auth': {'user': 'admin', 'password': 'Service@12'},
            'default_batch_size': 100
        }
    ]
}

def mock__process_and_cache_relation_types(params):
    return

def mock__process_components(params):
    return

def mock__process_component_relations(params):
    return


def mock_collect_components():
    ''' Mock behaviour(response) from ServiceNow API for Components(CIs)'''
    response = {'result': [{'sys_class_name': 'cmdb_ci_computer', 'sys_id': '00a96c0d3790200044e0bfc8bcbe5db4',
                            'sys_created_on': '2012-02-18 08:14:21', 'name': 'MacBook Pro 15'}]}
    return json.dumps(response)

def mock_relation_types():
    '''Mock behaviour for relation types'''
    response = {'result': [{'parent_descriptor': 'Cools', 'sys_id': '53979c53c0a801640116ad2044643fb2'}]}
    return json.dumps(response)

def mock_relation_components():
    ''' Mock response from ServiceNow API for relation between components'''
    response = {'result': [
        {'type': {'link': 'https://dev60476.service-now.com/api/now/table/cmdb_rel_type/1a9cb166f1571100a92eb60da2bce5c5',
                  'value': '1a9cb166f1571100a92eb60da2bce5c5'},
         'parent': {'link': 'https://dev60476.service-now.com/api/now/table/cmdb_ci/451047c6c0a8016400de0ae6df9b9d76',
                    'value': '451047c6c0a8016400de0ae6df9b9d76'},
         'child': {'link': 'https://dev60476.service-now.com/api/now/table/cmdb_ci/53979c53c0a801640116ad2044643fb2',
                   'value': '53979c53c0a801640116ad2044643fb2'}}
        ]}
    return json.dumps(response)

# NOTE: Feel free to declare multiple test classes if needed

@attr(requires='servicenow')
class TestServicenow(AgentCheckTest):
    """Basic Test for servicenow integration."""
    CHECK_NAME = 'servicenow'
    SERVICE_CHECK_NAME = "servicenow.cmdb.topology_information"

    def test_check(self):
        """
        Testing Servicenow check.
        """

        self.base_url = instance['url']

        self.run_check(CONFIG, mocks={
                '_process_and_cache_relation_types': mock__process_and_cache_relation_types,
                '_process_components': mock__process_components,
                '_process_component_relations': mock__process_component_relations
            })

        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(len(instances[0]['components']), 0)
        self.assertEquals(len(instances[0]['relations']), 0)

    def test_make_service_check_ok(self):
        """
        Testing make_service_check function to return service check ok
        """
        params = {'service_check_done': False}
        status = AgentCheck.OK
        tags = ["url:https://dev60476.service-now.com"]
        msg = "ServiceNow CMDB instance detected at https://dev60476.service-now.com"
        self.load_check(instance)
        self.check.make_service_check(params, status, tags, msg)
        sc = self.check.get_service_checks()

        self.assertEqual(len(sc), 1)
        self.assertEqual(sc[0]['check'], self.SERVICE_CHECK_NAME)
        self.assertEqual(sc[0]['status'], AgentCheck.OK)

    def test_make_service_check_exception(self):
        """
        Testing make_service_check function to throw check exception
        """
        params = {'service_check_done': False}
        status = AgentCheck.CRITICAL
        tags = ["url:https://dev60476.service-now.com"]
        msg = "Timeout when hitting https://dev60476.service-now.com"

        self.load_check(instance)
        self.check.make_service_check = mock.MagicMock()
        self.check.make_service_check.side_effect = [CheckException]
        self.assertRaises(CheckException, self.check.make_service_check, params, status, tags, msg)

    def test_collect_components(self):
        """
        Test to raise a check exception when hitting API
        :return:
        """
        self.load_check(CONFIG)
        params = {'base_url': instance.get('url'), 'auth': ('admin', 'Service@123'),
                  'timeout': 10, 'service_check_done': False}
        self.assertRaises(CheckException, self.check._collect_components, params)

    def test_process_components(self):
        """
        Test _process_components to return topology for components
        """
        self.load_check(CONFIG)
        params = {'instance_key': {"key": "dummy"}, 'instance_tags': []}
        self.check._collect_components = mock.MagicMock()
        self.check._collect_components.return_value = json.loads(mock_collect_components())
        self.check._process_components(params)
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]['components'][0]['type']['name'], 'cmdb_ci_computer')
        self.assertEquals(len(instances[0]['relations']), 0)

    def test_collect_relation_types(self):
        """
        Test to raise a check exception when collecting relation types
        """
        self.load_check(CONFIG)
        params = {'base_url': instance.get('url'), 'auth': ('admin', 'Service@123'),
                  'timeout': 10, 'service_check_done': False}
        self.assertRaises(CheckException, self.check._collect_relation_types, params)

    def test_process_and_cache_relation_types(self):
        """
        Test to collect relation types from ServiceNow API and put in relation_types
        """
        self.load_check(CONFIG)
        params = {'relation_types': {}}
        self.check._collect_relation_types = mock.MagicMock()
        self.check._collect_relation_types.return_value = json.loads(mock_relation_types())
        self.check._process_and_cache_relation_types(params)

        self.assertEqual(len(params['relation_types']), 1)

    def test_collect_component_relations(self):
        """
        Test to raise a check Exception while collecting component relations from ServiceNow API
        """
        self.load_check(CONFIG)
        params = {'base_url': instance.get('url'), 'auth': ('admin', 'Service@123'),
                  'timeout': 10, 'service_check_done': False}
        self.assertRaises(CheckException, self.check._collect_component_relations, params, 0, 100)

    def test_process_component_relations(self):
        """
        Test to collect the component relations and process it as a topology
        :return:
        """
        self.load_check(CONFIG)
        params = {'relation_types': {'1a9cb166f1571100a92eb60da2bce5c5': 'Cools'}, 'instance_tags': [], 'instance_key': {"key": "dummy"},
                  'timeout': 10, 'service_check_done': False, 'batch_size': instance.get('default_batch_size'),}
        self.check._collect_component_relations = mock.MagicMock()
        self.check._collect_component_relations.return_value = json.loads(mock_relation_components())
        self.check._process_component_relations(params)
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(len(instances[0]['components']), 0)
        self.assertEquals(instances[0]['relations'][0]['type']['name'], 'Cools')

    @mock.patch('check.requests.get')
    def test__get_json(self, mock_req_get):
        """
        Test to check the method _get_json with positive response and get a OK service check
        """
        url = instance.get('url') + "/api/now/table/cmdb_ci"
        params = {'service_check_done': False}
        auth = (instance.get('basic_auth')['user'], instance.get('basic_auth')['password'])
        self.load_check(instance)
        mock_req_get.return_value = mock.MagicMock(status_code=200, response=json.dumps({'key':'value'}))
        self.check._get_json(params, url, timeout=10, auth=auth)
        sc = self.check.get_service_checks()
        self.assertEqual(len(sc), 1)
        self.assertEqual(sc[0]['check'], self.SERVICE_CHECK_NAME)
        self.assertEqual(sc[0]['status'], AgentCheck.OK)
