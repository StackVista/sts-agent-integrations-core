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
from check import ServicenowCheck
from checks import AgentCheck, CheckException


instance = {
    'url': "https://dev6047.service-now.com",
    'basic_auth': {'user': 'admin', 'password': 'Service@123'}
}

CONFIG = {
    'init_config': {'default_timeout': 10, 'min_collection_interval': 5},
    'instances': [
        {
            'url': "https://dev60479.service-now.com",
            'basic_auth': {'user': 'admin', 'password': 'Service@12'}
        }
    ]
}

def mock__process_and_cache_relation_types():
    return

def mock__process_components():
    return

def mock__process_component_relations():
    return


def get_json():
    response = {'result': [{'sys_class_name': 'cmdb_ci_computer', 'sys_id': '00a96c0d3790200044e0bfc8bcbe5db4',
                            'sys_created_on': '2012-02-18 08:14:21', 'name': 'MacBook Pro 15'}]}
    return json.dumps(response)

# NOTE: Feel free to declare multiple test classes if needed

@attr(requires='servicenow')
class TestServicenow(AgentCheckTest):
    """Basic Test for servicenow integration."""
    CHECK_NAME = 'servicenow'
    SERVICE_CHECK_NAME = "servicenow.cmdb.topology_information"
    service_check_done = False


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

    def test_make_service_check_ok(self):
        status = AgentCheck.OK
        tags = ["url:https://dev60476.service-now.com"]
        msg = "ServiceNow CMDB instance detected at https://dev60476.service-now.com"
        self.load_check(instance)
        self.check.make_service_check(status, tags, msg)
        sc = self.check.get_service_checks()

        self.assertEqual(len(sc), 1)
        self.assertEqual(sc[0]['check'], self.SERVICE_CHECK_NAME)
        self.assertEqual(sc[0]['status'], AgentCheck.OK)

    def test_make_service_check_exception(self):
        status = AgentCheck.CRITICAL
        tags = ["url:https://dev60476.service-now.com"]
        msg = "Timeout when hitting https://dev60476.service-now.com"

        self.load_check(instance)
        self.check.make_service_check = mock.MagicMock()
        self.check.make_service_check.side_effect = [CheckException]
        self.assertRaises(CheckException, self.check.make_service_check, status, tags, msg)

    def test_collect_components(self):
        self.load_check(CONFIG)
        self.check.base_url = instance.get('url')
        self.assertRaises(CheckException, self.check._collect_components)

    def test_process_components(self):
        self.load_check(CONFIG)
        self.check._collect_components = mock.MagicMock()
        self.check._collect_components.side_effect = json.loads(get_json())
        self.check._process_components()
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)

    # def test__get_json(self):
    #     url = instance.get('url') + "/api/now/table/cmdb_ci"
    #     auth = (instance.get('basic_auth')['user'], instance.get('basic_auth')['password'])
    #     with mock.patch('check.requests.get') as r:
    #         self.load_check(instance)
    #         r.side_effect = [{}, {200}]
    #         self.check._get_json(url, timeout=10, auth=auth)
    #         sc = self.check.get_service_checks()
    #         self.assertEqual(len(sc), 1)
    #         self.assertEqual(sc[0]['check'], self.SERVICE_CHECK_NAME)
    #         self.assertEqual(sc[0]['status'], AgentCheck.OK)
