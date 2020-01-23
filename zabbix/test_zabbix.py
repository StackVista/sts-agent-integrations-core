# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
import os

# 3p

# project
from tests.checks.common import AgentCheckTest
from checks import CheckException
import mock

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), 'ci')


class TestZabbixInvalidConfig(AgentCheckTest):
    CHECK_NAME = 'zabbix'

    def test_missing_zabbix_url(self):
        config = {
            'init_config': {},
            'instances': [
                {
                    'user': 'Admin',
                    'password': 'zabbix'
                }
            ]
        }

        with self.assertRaises(CheckException) as context:
            self.run_check(config)
        self.assertEqual('Missing API url in configuration.', str(context.exception))

    def test_missing_zabbix_user(self):
        config = {
            'init_config': {},
            'instances': [
                {
                    'url': "http://host/zabbix/api_jsonrpc.php",
                    'password': 'zabbix'
                }
            ]
        }

        with self.assertRaises(CheckException) as context:
            self.run_check(config)
        self.assertEqual('Missing API user in configuration.', str(context.exception))

    def test_missing_zabbix_password(self):
        config = {
            'init_config': {},
            'instances': [
                {
                    'url': "http://host/zabbix/api_jsonrpc.php",
                    'user': 'Admin'
                }
            ]
        }

        with self.assertRaises(CheckException) as context:
            self.run_check(config)
        self.assertEqual('Missing API password in configuration.', str(context.exception))


class TestZabbix(AgentCheckTest):
    CHECK_NAME = 'zabbix'

    _config = {
        'init_config': {},
        'instances': [
            {
                'url': "http://host/zabbix/api_jsonrpc.php",
                'user': 'Admin',
                'password': 'zabbix'
            }
        ]
    }

    @staticmethod
    def _apiinfo_response():
        return {
            "jsonrpc": "2.0",
            "result": ["4.0.4"],
            "id": 1
        }

    @staticmethod
    def _zabbix_host_response():
        return {
            "jsonrpc": "2.0",
            "result": [
                {
                    "hostid": "10084",
                    "host": "zabbix01.example.com",
                    "name": "Zabbix server",
                    "groups": [
                        {
                            "groupid": "4",
                            "name": "Zabbix servers"
                        }
                    ]
                }
            ],
            "id": 1
        }

    @staticmethod
    def _zabbix_problem():
        return {
            "jsonrpc": "2.0",
            "result": [
                {
                    "eventid": "14",
                    "source": "0",
                    "object": "0",
                    "objectid": "13491",
                    "clock": "1549878981",
                    "ns": "221836547",
                    "r_eventid": "0",
                    "r_clock": "0",
                    "r_ns": "0",
                    "correlationid": "0",
                    "userid": "0",
                    "name": "Zabbix agent on Zabbix server is unreachable for 5 minutes",
                    "acknowledged": "0",
                    "severity": "3",
                    "acknowledges": [],
                    "suppressed": 0
                }
            ],
            "id": 1
        }

    @staticmethod
    def _zabbix_trigger():
        return {
            "jsonrpc": "2.0",
            "result": [
                {
                    "triggerid": "13491",
                    "expression": "{12900}=1",
                    "description": "Zabbix agent on {HOST.NAME} is unreachable for 5 minutes",
                    "url": "",
                    "status": "0",
                    "value": "1",
                    "priority": "3",
                    "lastchange": "1549878981",
                    "comments": "",
                    "error": "",
                    "templateid": "10047",
                    "type": "0",
                    "state": "0",
                    "flags": "0",
                    "recovery_mode": "0",
                    "recovery_expression": "",
                    "correlation_mode": "0",
                    "correlation_tag": "",
                    "manual_close": "0"
                }
            ],
            "id": 1
        }

    @staticmethod
    def _zabbix_event():
        return {
            "jsonrpc": "2.0",
            "result": [
                {
                    "eventid": "14",
                    "value": "1",
                    "severity": "3",
                    "acknowledged": "0",
                    "hosts": [
                        {
                            "hostid": "10084"
                        }
                    ],
                    "relatedObject": {
                        "triggerid": "13491",
                        "description": "Zabbix agent on {HOST.NAME} is unreachable for 5 minutes",
                        "priority": "3"
                    }
                }
            ],
            "id": 1
        }

    def test_zabbix_topology_hosts(self):
        def _mocked_method_request(url, name, auth=None, params={}, request_id=1):
            if name == "apiinfo.version":
                return self._apiinfo_response()
            elif name == "host.get":
                return self._zabbix_host_response()
            else:
                self.fail("TEST FAILED on making invalid request")

        self.run_check(self._config, mocks={
            'method_request': _mocked_method_request,
            'login': lambda url, user, password: "dummyauthtoken",
            'retrieve_problems': lambda url, auth: [],
            'retrieve_events': lambda url, auth, event_ids: []
        })
        topo_instances = self.check.get_topology_instances()
        self.assertEqual(len(topo_instances), 1)
        self.assertEqual(len(topo_instances[0]['components']), 1)
        self.assertEqual(len(topo_instances[0]['relations']), 0)

        component = topo_instances[0]['components'][0]
        self.assertEqual(component['externalId'], 'urn:host:/zabbix01.example.com')
        self.assertEqual(component['type']['name'], 'zabbix_host')
        self.assertEqual(component['data']['name'], 'Zabbix server')
        self.assertEqual(component['data']['host_id'], '10084')
        self.assertEqual(component['data']['host'], 'zabbix01.example.com')
        self.assertEqual(component['data']['layer'], 'Host')
        self.assertEqual(component['data']['domain'], 'Zabbix servers')
        self.assertEqual(component['data']['identifiers'], ['zabbix01.example.com'])
        self.assertEqual(component['data']['environment'], 'Production')
        self.assertEqual(component['data']['host_groups'], ['Zabbix servers'])

        labels = component['data']['labels']
        for label in ['zabbix', 'host group:Zabbix servers']:
            if label not in labels:
                self.fail("Component does not have label '%s'." % label)

    def test_zabbix_topology_non_default_environment(self):
        def _mocked_method_request(url, name, auth=None, params={}, request_id=1):
            if name == "apiinfo.version":
                return self._apiinfo_response()
            elif name == "host.get":
                return self._zabbix_host_response()
            else:
                self.fail("TEST FAILED on making invalid request")

        config = self._config
        config['instances'][0]['stackstate_environment'] = 'MyTestEnvironment'

        self.run_check(config, mocks={
            'method_request': _mocked_method_request,
            'login': lambda url, user, password: "dummyauthtoken",
            'retrieve_problems': lambda url, auth: [],
            'retrieve_events': lambda url, auth, event_ids: []
        })
        topo_instances = self.check.get_topology_instances()
        self.assertEqual(len(topo_instances), 1)
        self.assertEqual(len(topo_instances[0]['components']), 1)
        self.assertEqual(len(topo_instances[0]['relations']), 0)

        component = topo_instances[0]['components'][0]
        self.assertEqual(component['data']['environment'], 'MyTestEnvironment')

        labels = component['data']['labels']
        for label in ['zabbix', 'host group:Zabbix servers']:
            if label not in labels:
                self.fail("Component does not have label '%s'." % label)

    def test_zabbix_topology_multiple_host_groups(self):
        """
        Zabbix hosts can be placed in multiple host groups.
        When there is only one host group we place the host component in the StackState domain with the host group's name.
        However, when there are multiple host groups we use StackState domain 'Zabbix'
        """

        def _mocked_method_request(url, name, auth=None, params={}, request_id=1):
            if name == "apiinfo.version":
                return self._apiinfo_response()
            elif name == "host.get":
                response = self._zabbix_host_response()
                response['result'][0]['groups'].append(
                    {
                        "groupid": "5",
                        "name": "MyHostGroup"
                    }
                )
                return response
            else:
                self.fail("TEST FAILED on making invalid request")

        self.run_check(self._config, mocks={
            'method_request': _mocked_method_request,
            'login': lambda url, user, password: "dummyauthtoken",
            'retrieve_problems': lambda url, auth: [],
            'retrieve_events': lambda url, auth, event_ids: []
        })
        topo_instances = self.check.get_topology_instances()
        self.assertEqual(len(topo_instances), 1)
        self.assertEqual(len(topo_instances[0]['components']), 1)
        self.assertEqual(len(topo_instances[0]['relations']), 0)

        component = topo_instances[0]['components'][0]
        self.assertEqual(component['data']['domain'], 'Zabbix')
        labels = component['data']['labels']
        for label in ['zabbix', 'host group:Zabbix servers', 'host group:MyHostGroup']:
            if label not in labels:
                self.fail("Component does not have label '%s'." % label)

    def test_zabbix_problems(self):
        def _mocked_method_request(url, name, auth=None, params={}, request_id=1):
            if name == "apiinfo.version":
                return self._apiinfo_response()
            elif name == "host.get":
                return self._zabbix_host_response()
            elif name == "problem.get":
                return self._zabbix_problem()
            elif name == "trigger.get":
                return self._zabbix_trigger()
            elif name == "event.get":
                return self._zabbix_event()
            else:
                self.fail("TEST FAILED on making invalid request")

        self.run_check(self._config, mocks={
            'method_request': _mocked_method_request,
            'login': lambda url, user, password: "dummyauthtoken",
        })

        self.assertEqual(len(self.events), 1)
        event = self.events[0]
        self.assertEqual(event['source_type_name'], 'Zabbix')
        tags = event['tags']

        for tag in ['host_id:10084', 'severity:3', "triggers:['Zabbix agent on {HOST.NAME} is unreachable for 5 minutes']", "host:zabbix01.example.com", "host_name:Zabbix server"]:
            if tag not in tags:
                self.fail("Event does not have tag '%s', got: %s." % (tag, tags))
        self.assertEqual(len(tags), 5)

    def test_zabbix_no_problems(self):
        """
        When there are no problems, we are expecting all host components to go to green.
        To make this happen we need to send an event that says all is OK.
        """
        def _mocked_method_request(url, name, auth=None, params={}, request_id=1):
            if name == "apiinfo.version":
                return self._apiinfo_response()
            elif name == "host.get":
                return self._zabbix_host_response()
            elif name == "problem.get":
                response = self._zabbix_problem()
                response['result'] = []
                return response
            else:
                self.fail("TEST FAILED on making invalid request")

        self.run_check(self._config, mocks={
            'method_request': _mocked_method_request,
            'login': lambda url, user, password: "dummyauthtoken",
        })

        self.assertEqual(len(self.events), 1)
        event = self.events[0]
        self.assertEqual(event['source_type_name'], 'Zabbix')
        tags = event['tags']

        for tag in ['host_id:10084', 'severity:0', "triggers:[]", "host:zabbix01.example.com", "host_name:Zabbix server"]:
            if tag not in tags:
                self.fail("Event does not have tag '%s', got: %s." % (tag, tags))
        self.assertEqual(len(tags), 5)

    def test_zabbix_determine_most_severe_state(self):
        """
            A host can have multiple active problems.
            From the active problems we determine the most severe state and send that to StackState
        """

        def _mocked_method_request(url, name, auth=None, params={}, request_id=1):
            if name == "apiinfo.version":
                return self._apiinfo_response()
            elif name == "host.get":
                return self._zabbix_host_response()
            elif name == "problem.get":
                response = self._zabbix_problem()
                response['result'].append({
                    "eventid": "100",
                    "source": "0",
                    "object": "0",
                    "objectid": "111",
                    "clock": "1549878981",
                    "ns": "221836547",
                    "r_eventid": "0",
                    "r_clock": "0",
                    "r_ns": "0",
                    "correlationid": "0",
                    "userid": "0",
                    "name": "My very own problem",
                    "acknowledged": "0",
                    "severity": "5",
                    "acknowledges": [],
                    "suppressed": 0
                })
                return response
            elif name == "trigger.get":
                return self._zabbix_trigger()
            elif name == "event.get":
                response = self._zabbix_event()
                response['result'].append({
                    "eventid": "100",
                    "value": "1",
                    "severity": "5",
                    "acknowledged": "0",
                    "hosts": [
                        {
                            "hostid": "10084"
                        }
                    ],
                    "relatedObject": {
                        "triggerid": "111",
                        "description": "My very own problem",
                        "priority": "5"
                    }
                })
                return response
            else:
                self.fail("TEST FAILED on making invalid request")

        self.run_check(self._config, mocks={
            'method_request': _mocked_method_request,
            'login': lambda url, user, password: "dummyauthtoken",
        })

        self.assertEqual(len(self.events), 1)
        event = self.events[0]
        self.assertEqual(event['source_type_name'], 'Zabbix')
        tags = event['tags']

        for tag in [
            'host_id:10084',
            'severity:5',
            "triggers:['Zabbix agent on {HOST.NAME} is unreachable for 5 minutes', 'My very own problem']",
            "host:zabbix01.example.com",
            "host_name:Zabbix server"
        ]:
            if tag not in tags:
                self.fail("Event does not have tag '%s', got: %s." % (tag, tags))
        self.assertEqual(len(tags), 5)

    def validate_requests_ssl_verify_setting(self, config_to_use, expected_verify_value):
        """
        Helper for testing whether the yaml setting ssl_verify is respected by mocking requests.get
        Mocking all the Zabbix functions that talk HTTP via requests.get, excluding the function `check_connection`
        Function check_connection is the first function that talks HTTP.
        """
        with mock.patch('requests.get') as mock_get:
            self.run_check(config_to_use, mocks={
                'login': lambda url, user, password: "dummyauthtoken",
                'retrieve_hosts': lambda x, y: [],
                'retrieve_problems': lambda url, auth: [],
                'retrieve_events': lambda url, auth, event_ids: []
            })
            mock_get.assert_called_once_with('http://host/zabbix/api_jsonrpc.php', json={'params': {}, 'jsonrpc': '2.0', 'method': 'apiinfo.version', 'id': 1}, verify=expected_verify_value)

    def test_zabbix_respect_false_ssl_verify(self):
        config = self._config
        config['instances'][0]['ssl_verify'] = False
        self.validate_requests_ssl_verify_setting(config, False)

    def test_zabbix_respect_true_ssl_verify(self):
        config = self._config
        config['instances'][0]['ssl_verify'] = True
        self.validate_requests_ssl_verify_setting(config, True)

    def test_zabbix_respect_default_ssl_verify(self):
        self.validate_requests_ssl_verify_setting(self._config, True)
