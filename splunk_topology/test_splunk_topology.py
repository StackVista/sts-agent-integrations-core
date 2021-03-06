# stdlib
import json
import os

from checks import CheckException, FinalizeException, TokenExpiredException
from tests.checks.common import AgentCheckTest, Fixtures

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), 'ci')


def _mocked_saved_searches(*args, **kwargs):
    return []


def _mocked_auth_session(instance_key):
    return "sessionKey1"


def _mocked_dispatch(*args, **kwargs):
    return args[1].name


class TestSplunkNoTopology(AgentCheckTest):
    """
    Splunk check should work in absence of topology
    """
    CHECK_NAME = 'splunk_topology'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'component_saved_searches': [],
                    'relation_saved_searches': []
                }
            ]
        }
        self.run_check(config, mocks={'_saved_searches':_mocked_saved_searches, '_auth_session': _mocked_auth_session})
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)


# Sid is equal to search name
def _mocked_dispatch_saved_search(*args, **kwargs):
    name = args[1].name
    if name == "dispatch_error":
        raise Exception("BOOM")
    return args[1].name


def _mocked_search(*args, **kwargs):
    # sid is set to saved search name
    sid = args[0]
    return [json.loads(Fixtures.read_file("%s.json" % sid, sdk_dir=FIXTURE_DIR))]


class TestSplunkTopology(AgentCheckTest):
    """
    Splunk check should work with component and relation data
    """
    CHECK_NAME = 'splunk_topology'

    def tear_down(self, url, qualifier):
        """
        Clear the persistent state from the system for next time
        """
        self.check.update_persistent_status(url, qualifier, None, 'clear')

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'component_saved_searches': [{
                        "name": "components",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [{
                        "name": "relations",
                        "parameters": {}
                    }],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session
        })

        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]['instance'], {"type":"splunk","url":"http://localhost:8089"})

        self.assertEqual(instances[0]['components'][0], {
            "externalId": u"vm_2_1",
            "type": {"name": u"vm"},
            "data": {
                u"running": True,
                u"_time": u"2017-03-06T14:55:54.000+00:00",
                "label.label1Key": "label1Value",
                "tags": ['result_tag1', 'mytag', 'mytag2']
            }
        })

        self.assertEqual(instances[0]['components'][1], {
            "externalId": u"server_2",
            "type": {"name": u"server"},
            "data": {
                u"description": u"My important server 2",
                u"_time": u"2017-03-06T14:55:54.000+00:00",
                "label.label2Key": "label2Value",
                "tags": ['result_tag2', 'mytag', 'mytag2']
            }
        })

        self.assertEquals(instances[0]['relations'][0], {
            "externalId": u"vm_2_1-HOSTED_ON-server_2",
            "type": {"name": u"HOSTED_ON"},
            "sourceId": u"vm_2_1",
            "targetId": u"server_2",
            "data": {
                u"description": u"Some relation",
                u"_time": u"2017-03-06T15:10:57.000+00:00",
                "tags": ['mytag', 'mytag2']
            }
        })

        self.assertEquals(instances[0]["start_snapshot"], True)
        self.assertEquals(instances[0]["stop_snapshot"], True)

        self.assertEquals(self.service_checks[0]['status'], 0, "service check should have status AgentCheck.OK")

    def test_checks_backward_compatibility(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'username': 'admin',
                    'password': 'admin',
                    'component_saved_searches': [{
                        "name": "components",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [{
                        "name": "relations",
                        "parameters": {}
                    }],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session
        })

        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]['instance'], {"type":"splunk","url":"http://localhost:8089"})

        self.assertEqual(instances[0]['components'][0], {
            "externalId": u"vm_2_1",
            "type": {"name": u"vm"},
            "data": {
                u"running": True,
                u"_time": u"2017-03-06T14:55:54.000+00:00",
                "label.label1Key": "label1Value",
                "tags": ['result_tag1', 'mytag', 'mytag2']
            }
        })

        self.assertEqual(instances[0]['components'][1], {
            "externalId": u"server_2",
            "type": {"name": u"server"},
            "data": {
                u"description": u"My important server 2",
                u"_time": u"2017-03-06T14:55:54.000+00:00",
                "label.label2Key": "label2Value",
                "tags": ['result_tag2', 'mytag', 'mytag2']
            }
        })

        self.assertEquals(instances[0]['relations'][0], {
            "externalId": u"vm_2_1-HOSTED_ON-server_2",
            "type": {"name": u"HOSTED_ON"},
            "sourceId": u"vm_2_1",
            "targetId": u"server_2",
            "data": {
                u"description": u"Some relation",
                u"_time": u"2017-03-06T15:10:57.000+00:00",
                "tags": ['mytag', 'mytag2']
            }
        })

        self.assertEquals(instances[0]["start_snapshot"], True)
        self.assertEquals(instances[0]["stop_snapshot"], True)

        self.assertEquals(self.service_checks[0]['status'], 0, "service check should have status AgentCheck.OK")

    def test_checks_backward_compatibility_with_new_conf(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'username': 'admin',
                    'password': 'admin',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'component_saved_searches': [{
                        "name": "components",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [{
                        "name": "relations",
                        "parameters": {}
                    }],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session
        })

        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]['instance'], {"type":"splunk","url":"http://localhost:8089"})

        self.assertEqual(instances[0]['components'][0], {
            "externalId": u"vm_2_1",
            "type": {"name": u"vm"},
            "data": {
                u"running": True,
                u"_time": u"2017-03-06T14:55:54.000+00:00",
                "label.label1Key": "label1Value",
                "tags": ['result_tag1', 'mytag', 'mytag2']
            }
        })

        self.assertEqual(instances[0]['components'][1], {
            "externalId": u"server_2",
            "type": {"name": u"server"},
            "data": {
                u"description": u"My important server 2",
                u"_time": u"2017-03-06T14:55:54.000+00:00",
                "label.label2Key": "label2Value",
                "tags": ['result_tag2', 'mytag', 'mytag2']
            }
        })

        self.assertEquals(instances[0]['relations'][0], {
            "externalId": u"vm_2_1-HOSTED_ON-server_2",
            "type": {"name": u"HOSTED_ON"},
            "sourceId": u"vm_2_1",
            "targetId": u"server_2",
            "data": {
                u"description": u"Some relation",
                u"_time": u"2017-03-06T15:10:57.000+00:00",
                "tags": ['mytag', 'mytag2']
            }
        })

        self.assertEquals(instances[0]["start_snapshot"], True)
        self.assertEquals(instances[0]["stop_snapshot"], True)

        self.assertEquals(self.service_checks[0]['status'], 0, "service check should have status AgentCheck.OK")

    def test_not_dispatch_sids_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089/',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'component_saved_searches': [{
                        "name": "components",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }
        instance = config.get('instances')[0]
        persist_status_key = instance.get('url')+"components"

        def _mocked_finalize_sid_none(*args, **kwargs):
            return None
        # Run the check first time and get the persistent status data
        self.run_check(config, mocks={
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session,
            '_dispatch': _mocked_dispatch
        })

        first_persistent_data = self.check._status().data.get(persist_status_key)

        # Run the check 2nd time and get the persistent status data
        self.run_check(config, mocks={
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session,
            '_dispatch': _mocked_dispatch,
            '_finalize_sid': _mocked_finalize_sid_none
        }, force_reload=True)

        second_persistent_data = self.check._status().data.get(persist_status_key)
        # The second run_check will finalize the previous saved search id and create a new one,
        # so we make sure this is the case
        self.assertEqual(first_persistent_data, second_persistent_data)

        def _mocked_finalize_sid_exception(*args, **kwargs):
            raise FinalizeException(None, "Error occured")

        thrown = False
        try:
            self.run_check(config, mocks={
                '_search': _mocked_search,
                '_saved_searches': _mocked_saved_searches,
                '_auth_session': _mocked_auth_session,
                '_dispatch': _mocked_dispatch,
                '_finalize_sid': _mocked_finalize_sid_exception
            }, force_reload=True)
        except CheckException:
            thrown = True

        self.assertTrue(thrown)
        self.assertEquals(self.service_checks[0]['status'], 2, "service check should have status AgentCheck.CRITICAL")

        # make sure the data still persists after exception raised
        self.assertIsNotNone(self.check._status().data.get(persist_status_key))

        # tear down the persistent data
        self.tear_down(instance.get('url'), "components")


class TestSplunkNoSnapshot(AgentCheckTest):
    """
    Splunk check should work with component and relation data
    """
    CHECK_NAME = 'splunk_topology'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'snapshot': False,
                    'component_saved_searches': [{
                        "name": "components",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [{
                        "name": "relations",
                        "parameters": {}
                    }],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session
        })

        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]['instance'], {"type":"splunk","url":"http://localhost:8089"})

        self.assertEqual(len(instances[0]['components']), 2)

        self.assertEquals(len(instances[0]['relations']), 1)

        self.assertFalse("start_snapshot" in instances[0])
        self.assertFalse("stop_snapshot" in instances[0])

        self.assertEquals(self.service_checks[0]['status'], 0, "service check should have status AgentCheck.OK")


class TestSplunkMinimalTopology(AgentCheckTest):
    """
    Splunk check should work with minimal component and relation data
    """
    CHECK_NAME = 'splunk_topology'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'component_saved_searches': [{
                        "name": "minimal_components",
                        "element_type": "component",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [{
                        "name": "minimal_relations",
                        "element_type": "relation",
                        "parameters": {}
                    }],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session
        })

        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]['instance'], {"type":"splunk","url":"http://localhost:8089"})

        self.assertEqual(instances[0]['components'][0], {
            "externalId": u"vm_2_1",
            "type": {"name": u"vm"},
            "data": {
                "tags": ['mytag', 'mytag2']
            }
        })

        self.assertEqual(instances[0]['components'][1], {
            "externalId": u"server_2",
            "type": {"name": u"server"},
            "data": {
                "tags": ['mytag', 'mytag2']
            }
        })

        self.assertEquals(instances[0]['relations'][0], {
            "externalId": u"vm_2_1-HOSTED_ON-server_2",
            "type": {"name": u"HOSTED_ON"},
            "sourceId": u"vm_2_1",
            "targetId": u"server_2",
            "data": {
                "tags": ['mytag', 'mytag2']
            }
        })

        self.assertEquals(self.service_checks[0]['status'], 0, "service check should have status AgentCheck.OK")


class TestSplunkIncompleteTopology(AgentCheckTest):
    """
    Splunk check should crash on incomplete data
    """
    CHECK_NAME = 'splunk_topology'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'component_saved_searches': [{
                        "name": "incomplete_components",
                        "element_type": "component",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [{
                        "name": "incomplete_relations",
                        "element_type": "relation",
                        "parameters": {}
                    }],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        thrown = False
        try:
            self.run_check(config, mocks={
                '_dispatch_saved_search': _mocked_dispatch_saved_search,
                '_search': _mocked_search,
                '_saved_searches': _mocked_saved_searches,
                '_auth_session': _mocked_auth_session
            })
        except CheckException:
            thrown = True

        self.assertTrue(thrown, "Retrieving incomplete data from splunk should throw")

        self.assertEquals(self.service_checks[0]['status'], 2, "service check should have status AgentCheck.CRITICAL")


def _mocked_partially_incomplete_search(*args, **kwargs):
    # sid is set to saved search name
    sid = args[0]
    return [json.loads(Fixtures.read_file("partially_incomplete_%s.json" % sid, sdk_dir=FIXTURE_DIR))]


class TestSplunkPartiallyIncompleteTopology(AgentCheckTest):
    """
    Splunk check should crash on incomplete data
    """
    CHECK_NAME = 'splunk_topology'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'component_saved_searches': [{
                        "name": "partially_incomplete_components",
                        "element_type": "component",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [{
                        "name": "partially_incomplete_relations",
                        "element_type": "relation",
                        "parameters": {}
                    }],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session
        })

        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]['instance'], {"type":"splunk","url":"http://localhost:8089"})
        self.assertEqual(len(instances[0]['components']), 1)
        self.assertEqual(len(instances[0]['relations']), 1)

        self.assertEqual(instances[0]['components'][0], {
            "externalId": u"vm_2_1",
            "type": {"name": u"vm"},
            "data": {
                "tags": ['mytag', 'mytag2']
            }
        })

        self.assertEquals(instances[0]['relations'][0], {
            "externalId": u"vm_2_1-HOSTED_ON-server_2",
            "type": {"name": u"HOSTED_ON"},
            "sourceId": u"vm_2_1",
            "targetId": u"server_2",
            "data": {
                "tags": ['mytag', 'mytag2']
            }
        })

        self.assertEquals(len(self.service_checks), 2)
        self.assertEquals(self.service_checks[0]['status'], 1, "service check should have status AgentCheck.WARNING")
        self.assertEquals(self.service_checks[0]['message'],
                          "The saved search 'partially_incomplete_components' contained 1 incomplete component records")
        self.assertEquals(self.service_checks[1]['status'], 1, "service check should have status AgentCheck.WARNING")
        self.assertEquals(self.service_checks[1]['message'],
                          "The saved search 'partially_incomplete_relations' contained 1 incomplete relation records")


def _mocked_partially_incomplete_and_incomplete_search(*args, **kwargs):
    # sid is set to saved search name
    sid = args[0]
    return [json.loads(Fixtures.read_file("partially_incomplete_%s.json" % sid, sdk_dir=FIXTURE_DIR)),
            json.loads(Fixtures.read_file("incomplete_%s.json" % sid, sdk_dir=FIXTURE_DIR))]


class TestSplunkPartiallyIncompleteAndIncompleteTopology(AgentCheckTest):
    """
    Splunk check should crash on incomplete data
    """
    CHECK_NAME = 'splunk_topology'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'component_saved_searches': [{
                        "name": "components",
                        "element_type": "component",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [{
                        "name": "relations",
                        "element_type": "relation",
                        "parameters": {}
                    }],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_partially_incomplete_and_incomplete_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session
        })

        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]['instance'], {"type":"splunk","url":"http://localhost:8089"})
        self.assertEqual(len(instances[0]['components']), 1)
        self.assertEqual(len(instances[0]['relations']), 1)

        self.assertEqual(instances[0]['components'][0], {
            "externalId": u"vm_2_1",
            "type": {"name": u"vm"},
            "data": {
                "tags": ['mytag', 'mytag2']
            }
        })

        self.assertEquals(instances[0]['relations'][0], {
            "externalId": u"vm_2_1-HOSTED_ON-server_2",
            "type": {"name": u"HOSTED_ON"},
            "sourceId": u"vm_2_1",
            "targetId": u"server_2",
            "data": {
                "tags": ['mytag', 'mytag2']
            }
        })

        self.assertEquals(len(self.service_checks), 2)
        self.assertEquals(self.service_checks[0]['status'], 1, "service check should have status AgentCheck.WARNING")
        self.assertEquals(self.service_checks[0]['message'],
                          "The saved search 'components' contained 3 incomplete component records")
        self.assertEquals(self.service_checks[1]['status'], 1, "service check should have status AgentCheck.WARNING")
        self.assertEquals(self.service_checks[1]['message'],
                          "The saved search 'relations' contained 2 incomplete relation records")


class TestSplunkTopologyPollingInterval(AgentCheckTest):
    """
    Test whether the splunk check properly implements the polling intervals
    """
    CHECK_NAME = 'splunk_topology'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'component_saved_searches': [{
                        "name": "components_fast",
                        "element_type": "component",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [{
                        "name": "relations_fast",
                        "element_type": "relation",
                        "parameters": {}
                    }],
                    'tags': ['mytag', 'mytag2']
                },
                {
                    'url': 'http://remotehost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'polling_interval_seconds': 30,
                    'component_saved_searches': [{
                        "name": "components_slow",
                        "element_type": "component",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [{
                        "name": "relations_slow",
                        "element_type": "relation",
                        "parameters": {}
                    }],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        # Used to validate which searches have been executed
        test_data = {
            "expected_searches": [],
            "time": 0,
            "throw": False
        }

        def _mocked_current_time_seconds():
            return test_data["time"]

        def _mocked_interval_search(*args, **kwargs):
            if test_data["throw"]:
                raise CheckException("Is broke it")

            sid = args[0]
            self.assertTrue(sid in test_data["expected_searches"])
            return [json.loads(Fixtures.read_file("empty.json", sdk_dir=FIXTURE_DIR))]

        test_mocks = {
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_interval_search,
            '_current_time_seconds': _mocked_current_time_seconds,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session
        }

        # Inital run
        test_data["expected_searches"] = ["components_fast", "relations_fast", "components_slow", "relations_slow"]
        test_data["time"] = 1
        self.run_check(config, mocks=test_mocks)
        self.check.get_topology_instances()

        # Only fast ones after 15 seconds
        test_data["expected_searches"] = ["components_fast", "relations_fast"]
        test_data["time"] = 20
        self.run_check(config, mocks=test_mocks)
        self.check.get_topology_instances()

        # Slow ones after 40 seconds aswell
        test_data["expected_searches"] = ["components_fast", "relations_fast", "components_slow", "relations_slow"]
        test_data["time"] = 40
        self.run_check(config, mocks=test_mocks)
        self.check.get_topology_instances()

        # Nothing should happen when throwing
        test_data["expected_searches"] = []
        test_data["time"] = 60
        test_data["throw"] = True

        thrown = False
        try:
            self.run_check(config, mocks=test_mocks)
        except CheckException:
            thrown = True
        self.check.get_topology_instances()
        self.assertTrue(thrown, "Expect thrown to be done from the mocked search")
        self.assertEquals(self.service_checks[0]['status'], 2, "service check should have status AgentCheck.CRITICAL")

        # Updating should happen asap after throw
        test_data["expected_searches"] = ["components_fast", "relations_fast"]
        test_data["time"] = 61
        test_data["throw"] = False
        self.run_check(config, mocks=test_mocks)
        self.check.get_topology_instances()

        self.assertEquals(self.service_checks[0]['status'], 0, "service check should have status AgentCheck.OK")

class TestSplunkTopologyErrorResponse(AgentCheckTest):
    """
    Splunk check should handle a FATAL message response
    """
    CHECK_NAME = 'splunk_topology'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'component_saved_searches': [{
                        "name": "error",
                        "element_type": "component",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        thrown = False
        try:
            self.run_check(config, mocks={
                '_dispatch_saved_search': _mocked_dispatch_saved_search,
                '_search': _mocked_search,
                '_saved_searches': _mocked_saved_searches,
                '_auth_session': _mocked_auth_session
            })
        except CheckException:
            thrown = True
        self.assertTrue(thrown, "Retrieving FATAL message from Splunk should throw.")

        self.assertEquals(self.service_checks[0]['status'], 2, "service check should have status AgentCheck.CRITICAL")


class TestSplunkSavedSearchesError(AgentCheckTest):
    """
    Splunk topology check should have a service check failure when getting an exception from saved searches
    """
    CHECK_NAME = 'splunk_topology'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'component_saved_searches': [{
                        "name": "error",
                        "element_type": "component",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        def _mocked_saved_searches(*args, **kwargs):
            raise Exception("Boom")

        thrown = False
        try:
            self.run_check(config, mocks={
                '_saved_searches': _mocked_saved_searches
            })
        except CheckException:
            thrown = True
        self.assertTrue(thrown, "Retrieving FATAL message from Splunk should throw.")
        self.assertEquals(self.service_checks[0]['status'], 2, "service check should have status AgentCheck.CRITICAL")


class TestSplunkSavedSearchesIgnoreError(AgentCheckTest):
    """
    Splunk topology check should ignore exception when getting an exception from saved searches
    """
    CHECK_NAME = 'splunk_topology'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'ignore_saved_search_errors': True,
                    'component_saved_searches': [{
                        "name": "error",
                        "element_type": "component",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        def _mocked_saved_searches(*args, **kwargs):
            raise Exception("Boom")

        thrown = False
        try:
            self.run_check(config, mocks={
                '_saved_searches': _mocked_saved_searches
            })
        except CheckException:
            thrown = True
        self.assertFalse(thrown)
        self.assertEquals(self.service_checks[0]['status'], 2, "service check should have status AgentCheck.CRITICAL")



class TestTopologyDataIsClearedOnFailure(AgentCheckTest):
    """
    Splunk topology check should clear all topology data when one or more saves searches fail.
    """
    CHECK_NAME = 'splunk_topology'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches_parallel': 1,
                    'component_saved_searches': [{
                        "name": "components",
                        "element_type": "component",
                        "parameters": {}
                    },{
                        "name": "components",
                        "element_type": "component",
                        "parameters": {}
                    },{
                        "name": "dispatch_error",
                        "element_type": "component",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        thrown = False

        try:
            self.run_check(config, mocks={
                '_dispatch_saved_search': _mocked_dispatch_saved_search,
                '_search': _mocked_search,
                '_saved_searches': _mocked_saved_searches,
                '_auth_session': _mocked_auth_session
            })
        except CheckException:
            thrown = True

        self.assertTrue(thrown, "Retrieving FATAL message from Splunk should throw.")
        self.assertEquals(self.service_checks[0]['status'], 2, "service check should have status AgentCheck.CRITICAL")

        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        instance = instances[0]

        self.assertEqual(instance['instance'], {"type":"splunk","url":"http://localhost:8089"})
        self.assertEqual(len(instance['components']), 0)
        self.assertEqual(len(instance['relations']), 0)


class TestSplunkWildcardTopology(AgentCheckTest):
    """
    Splunk check should work with component and relation data
    """
    CHECK_NAME = 'splunk_topology'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'polling_interval_seconds': 0,
                    'component_saved_searches': [{
                        "match": "comp.*",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [{
                        "match": "rela.*",
                        "parameters": {}
                    }],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        data = {
            'saved_searches': []
        }

        def _mocked_saved_searches(*args, **kwargs):
            return data['saved_searches']

        # Add the saved searches
        data['saved_searches'] = ["components", "relations"]
        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session
        })
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]['instance'], {"type":"splunk","url":"http://localhost:8089"})
        self.assertEqual(len(instances[0]['components']), 2)
        self.assertEquals(len(instances[0]['relations']), 1)

        self.assertEquals(self.service_checks[0]['status'], 0, "service check should have status AgentCheck.OK")

        # Remove the saved searches
        data['saved_searches'] = []
        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]['instance'], {"type":"splunk","url":"http://localhost:8089"})
        self.assertEqual(len(instances[0]['components']), 0)
        self.assertEquals(len(instances[0]['relations']), 0)

        self.assertEquals(self.service_checks[0]['status'], 0, "service check should have status AgentCheck.OK")


class TestSplunkTopologyRespectParallelDispatches(AgentCheckTest):
    CHECK_NAME = 'splunk_topology'

    def test_checks(self):
        self.maxDiff = None

        saved_searches_parallel = 2

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches_parallel': saved_searches_parallel,
                    'component_saved_searches': [
                        {"name": "savedsearch1", "element_type": "component", "parameters": {}},
                        {"name": "savedsearch2", "element_type": "component", "parameters": {}},
                        {"name": "savedsearch3", "element_type": "component", "parameters": {}}
                    ],
                    'relation_saved_searches': [
                        {"name": "savedsearch4", "element_type": "relation", "parameters": {}},
                        {"name": "savedsearch5", "element_type": "relation", "parameters": {}}
                    ]
                }
            ]
        }

        self.expected_sid_increment = 1

        def _mock_dispatch_and_await_search(instance, saved_searches):
            self.assertLessEqual(len(saved_searches), saved_searches_parallel, "Did not respect the configured saved_searches_parallel setting, got value: %i" % len(saved_searches))

            for saved_search in saved_searches:
                result = saved_search.name
                expected = "savedsearch%i" % self.expected_sid_increment
                self.assertEquals(result, expected)
                self.expected_sid_increment += 1

            return True

        self.run_check(config, mocks={
            '_dispatch_and_await_search': _mock_dispatch_and_await_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session
        })


class TestSplunkDefaults(AgentCheckTest):
    CHECK_NAME = 'splunk_topology'

    def test_default_parameters(self):
        """
        when no default parameters are provided, the code should provide the parameters
        """
        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'component_saved_searches': [{
                        "name": "components"
                    }],
                    'relation_saved_searches': [{
                        "name": "relations"
                    }]
                }
            ]
        }
        expected_default_parameters = {'dispatch.now': True, 'force_dispatch': True}

        def _mocked_auth_session_to_check_instance_config(instance):
            for saved_search in instance.saved_searches.searches:
                self.assertEqual(saved_search.parameters, expected_default_parameters, msg="Unexpected default parameters for saved search: %s" % saved_search.name)
            return "sessionKey1"

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session_to_check_instance_config
        })
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)

    def test_non_default_parameters(self):
        """
        when non default parameters are provided, the code should respect them.
        """
        config = {
            'init_config': {
                'default_parameters': {
                    'respect': 'me'
                }
            },
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'component_saved_searches': [{
                        "name": "components"
                    }],
                    'relation_saved_searches': [{
                        "name": "relations"
                    }]
                }
            ]
        }
        expected_default_parameters = {'respect': 'me'}

        def _mocked_auth_session_to_check_instance_config(instance):
            for saved_search in instance.saved_searches.searches:
                self.assertEqual(saved_search.parameters, expected_default_parameters, msg="Unexpected non-default parameters for saved search: %s" % saved_search.name)
            return "sessionKey1"

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session_to_check_instance_config
        })
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)

    def test_overwrite_default_parameters(self):
        """
        when default parameters are overwritten, the code should respect them.
        """
        config = {
            'init_config': {
                'default_parameters': {
                    'default_should': 'be ignored'
                }
            },
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'component_saved_searches': [{
                        "name": "components",
                        "parameters": {
                            "respect": "me"
                        }
                    }],
                    'relation_saved_searches': [{
                        "name": "relations",
                        "parameters": {
                            "respect": "me"
                        }
                    }]
                }
            ]
        }
        expected_default_parameters = {'respect': 'me'}

        def _mocked_auth_session_to_check_instance_config(instance):
            for saved_search in instance.saved_searches.searches:
                self.assertEqual(saved_search.parameters, expected_default_parameters, msg="Unexpected overwritten default parameters for saved search: %s" % saved_search.name)
            return "sessionKey1"

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session_to_check_instance_config
        })
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)


class TestSplunkContinue(AgentCheckTest):
    CHECK_NAME = 'splunk_topology'

    def test_check_exception_continue(self):
        """
        When 1 saved search fails with Check Exception, the code should continue and send topology.
        """
        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'ignore_saved_search_errors': True,
                    'component_saved_searches': [
                        {"name": "components", "search_max_retry_count": 0, "parameters": {}},
                        {"name": "components12", "search_max_retry_count": 0, "parameters": {}}
                    ],
                    'relation_saved_searches': []
                }
            ]
        }

        def _mocked_check_exception_search(*args, **kwargs):
            sid = args[0]
            if sid == "components12":
                raise CheckException("maximum retries reached for saved search " + str(sid))
            return [json.loads(Fixtures.read_file("%s.json" % sid, sdk_dir=FIXTURE_DIR))]

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_check_exception_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session
        })
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        # Even second saved search failed but topology reported from first saved search
        self.assertEqual(len(instances[0]['components']), 2)
        # Check if all components came from first saved search
        self.assertEqual(instances[0]['components'][0]['externalId'], "vm_2_1")
        self.assertEqual(instances[0]['components'][0]['type']['name'], "vm")
        self.assertEqual(instances[0]['components'][1]['externalId'], "server_2")
        self.assertEqual(instances[0]['components'][1]['type']['name'], "server")

        self.assertEquals(len(instances[0]['relations']), 0)
        # second saved search throws a check exception for maximum retries data and report a service check
        self.assertEquals(self.service_checks[0]['status'], 1, "service check should have status AgentCheck.WARNING")
        # check if still the check continued and finished
        self.assertEqual(instances[0]["stop_snapshot"], True)
        self.assertEqual(instances[0]["start_snapshot"], True)

    def test_check_exception_false_continue(self):
        """
        When 1 saved search fails with Check Exception, the code should break and clear the whole topology.
        """
        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'ignore_saved_search_errors': False,
                    'component_saved_searches': [
                        {"name": "components", "search_max_retry_count": 0, "parameters": {}},
                        {"name": "components12", "search_max_retry_count": 0, "parameters": {}}
                    ],
                    'relation_saved_searches': []
                }
            ]
        }

        def _mocked_check_exception_search(*args, **kwargs):
            sid = args[0]
            if sid == "components12":
                raise CheckException("maximum retries reached for saved search " + str(sid))
            return [json.loads(Fixtures.read_file("%s.json" % sid, sdk_dir=FIXTURE_DIR))]
        thrown = False
        try:
            self.run_check(config, mocks={
                '_dispatch_saved_search': _mocked_dispatch_saved_search,
                '_search': _mocked_check_exception_search,
                '_saved_searches': _mocked_saved_searches,
                '_auth_session': _mocked_auth_session
            })
        except Exception:
            # Catch exception thrown from check as the ignore_saved_search_errors flag is False
            thrown = True

        self.assertTrue(thrown, "maximum retries reached for saved search components12")
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        # Even first saved search reported topology but not sent because the ignore_saved_search_errors flag was False
        self.assertEqual(len(instances[0]['components']), 0)
        self.assertEquals(len(instances[0]['relations']), 0)
        # second saved search throws a check exception for maximum retries data and report a service check
        self.assertEquals(self.service_checks[0]['status'], 2, "service check should have status AgentCheck.CRITICAL")

    def test_check_exception_fail_count_continue(self):
        """
        When both saved search fails with Check Exception, the code should continue and send topology.
        """
        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'ignore_saved_search_errors': True,
                    'component_saved_searches': [
                        {"name": "components", "search_max_retry_count": 0, "parameters": {}},
                        {"name": "components12", "search_max_retry_count": 0, "parameters": {}}
                    ],
                    'relation_saved_searches': []
                }
            ]
        }

        def _mocked_check_exception_search(*args, **kwargs):
            sid = args[0]
            if sid == "components12":
                raise CheckException("maximum retries reached for saved search " + str(sid))
            return [json.loads(Fixtures.read_file("%s.json" % sid, sdk_dir=FIXTURE_DIR))]

        def _mocked_extract_components(*args, **kwargs):
            return 2

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_check_exception_search,
            '_saved_searches': _mocked_saved_searches,
            '_extract_components': _mocked_extract_components,
            '_auth_session': _mocked_auth_session
        })
        instance = self.check.get_topology_instances()
        self.assertEqual(len(instance), 1)
        # first saved search throws an exception for incomplete data and report a service check
        self.assertEquals(self.service_checks[0]['status'], 1, "service check should have status AgentCheck.WARNING")
        self.assertEquals(self.service_checks[0]['message'], "All result of saved search 'components' contained incomplete data")
        # second saved search throws a check exception for maximum retries and report a service check
        self.assertEquals(self.service_checks[1]['status'], 1, "service check should have status AgentCheck.WARNING")
        self.assertEquals(self.service_checks[1]['message'], "maximum retries reached for saved search components12")
        # Both saved search failed so there should be no components and relations
        self.assertEqual(len(instance[0]['components']), 0)
        self.assertEqual(len(instance[0]['relations']), 0)
        # check if the check continued and finished
        self.assertEqual(instance[0]["stop_snapshot"], True)
        self.assertEqual(instance[0]["start_snapshot"], True)


class TestSplunkTokenBasedAuth(AgentCheckTest):

    CHECK_NAME = 'splunk_topology'

    def test_check_valid_initial_token(self):
        """
            Splunk topology check should work with valid initial token
        """

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'token_auth': {
                            'name': "admin",
                            'initial_token': "dsfdgfhgjhkjuyr567uhfe345ythu7y6tre456sdx",
                            'audience': "search",
                            'renewal_days': 10
                        }
                    },
                    'component_saved_searches': [{
                        "name": "components",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        def _mocked_token_auth_session(*args):
            return False

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_token_auth_session': _mocked_token_auth_session
        })

        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]['instance'], {"type":"splunk","url":"http://localhost:8089"})

        self.assertEqual(instances[0]['components'][0], {
            "externalId": u"vm_2_1",
            "type": {"name": u"vm"},
            "data": {
                u"running": True,
                u"_time": u"2017-03-06T14:55:54.000+00:00",
                "label.label1Key": "label1Value",
                "tags": ['result_tag1', 'mytag', 'mytag2']
            }
        })

        self.assertEqual(instances[0]['components'][1], {
            "externalId": u"server_2",
            "type": {"name": u"server"},
            "data": {
                u"description": u"My important server 2",
                u"_time": u"2017-03-06T14:55:54.000+00:00",
                "label.label2Key": "label2Value",
                "tags": ['result_tag2', 'mytag', 'mytag2']
            }
        })

        self.assertEquals(instances[0]["start_snapshot"], True)
        self.assertEquals(instances[0]["stop_snapshot"], True)

        self.assertEquals(self.service_checks[0]['status'], 0, "service check should have status AgentCheck.OK")

    def test_check_invalid_initial_token(self):
        """
            Splunk check should not work with invalid initial token and stop the check
        """
        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'token_auth': {
                            'name': "admin",
                            'initial_token': "dsfdgfhgjhkjuyr567uhfe345ythu7y6tre456sdx",
                            'audience': "search",
                            'renewal_days': 10
                        }
                    },
                    'component_saved_searches': [{
                        "name": "components",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        def _mocked_token_auth_session(*args):
            raise TokenExpiredException("Current in use authentication token is expired. Please provide a valid "
                                        "token in the YAML and restart the Agent")

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_token_auth_session': _mocked_token_auth_session
        })

        msg = "Current in use authentication token is expired. Please provide a valid token in the YAML and restart " \
              "the Agent"
        # Invalid token should throw a service check with proper message
        self.assertEquals(self.service_checks[0]['status'], 2, msg)
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        # there should be no topology as the check stopped while validating token
        self.assertEqual(len(instances[0]['components']), 0)

    def test_check_audience_param_not_set(self):
        """
            Splunk topology check should fail and raise exception when audience param is not set
        """

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'token_auth': {
                            'name': "admin",
                            'token': "dsfdgfhgjhkjuyr567uhfe345ythu7y6tre456sdx"
                        },
                    },
                    'component_saved_searches': [{
                        "name": "components",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        check = False

        try:
            self.run_check(config, mocks={
                '_dispatch_saved_search': _mocked_dispatch_saved_search,
                '_search': _mocked_search,
                '_saved_searches': _mocked_saved_searches,
            })
        except CheckException:
            check = True

        self.assertTrue(check, msg='Splunk topology instance missing "authentication.token_auth.audience" value')

    def test_check_name_param_not_set(self):
        """
            Splunk topology check should fail and raise exception when name param is not set
        """

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'token_auth': {
                            'audience': "admin",
                            'token': "dsfdgfhgjhkjuyr567uhfe345ythu7y6tre456sdx"
                        },
                    },
                    'component_saved_searches': [{
                        "name": "components",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        check = False
        try:
            self.run_check(config, mocks={
                '_dispatch_saved_search': _mocked_dispatch_saved_search,
                '_search': _mocked_search,
                '_saved_searches': _mocked_saved_searches,
            })
        except CheckException:
            check = True

        self.assertTrue(check, msg='Splunk topology instance missing "authentication.token_auth.name" value')

    def test_check_token_auth_preferred_over_basic_auth(self):
        """
            Splunk topology check should prefer Token based authentication over Basic auth mechanism
        """

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        },
                        'token_auth': {
                            'name': "api-admin",
                            'initial_token': "dsfdgfhgjhkjuyr567uhfe345ythu7y6tre456sdx",
                            'audience': "admin",
                            'renewal_days': 10
                        }
                    },
                    'component_saved_searches': [{
                        "name": "components",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        def _mocked_token_auth_session(*args):
            return None

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_token_auth_session': _mocked_token_auth_session
        })

        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]['instance'], {"type":"splunk","url":"http://localhost:8089"})

        self.assertEqual(instances[0]['components'][0], {
            "externalId": u"vm_2_1",
            "type": {"name": u"vm"},
            "data": {
                u"running": True,
                u"_time": u"2017-03-06T14:55:54.000+00:00",
                "label.label1Key": "label1Value",
                "tags": ['result_tag1', 'mytag', 'mytag2']
            }
        })

        self.assertEqual(instances[0]['components'][1], {
            "externalId": u"server_2",
            "type": {"name": u"server"},
            "data": {
                u"description": u"My important server 2",
                u"_time": u"2017-03-06T14:55:54.000+00:00",
                "label.label2Key": "label2Value",
                "tags": ['result_tag2', 'mytag', 'mytag2']
            }
        })

        self.assertEquals(instances[0]["start_snapshot"], True)
        self.assertEquals(instances[0]["stop_snapshot"], True)

        self.assertEquals(self.service_checks[0]['status'], 0, "service check should have status AgentCheck.OK")
        # clear the in memory token
        self.check.status.data.clear()
        self.check.status.persist("splunk_topology")

    def test_check_memory_token_expired(self):
        """
            Splunk topology check should fail when memory token is expired itself.
        """

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        },
                        'token_auth': {
                            'name': "api-admin",
                            'initial_token': "dsfdgfhgjhkjuyr567uhfe345ythu7y6tre456sdx",
                            'audience': "admin",
                            'renewal_days': 10
                        }
                    },
                    'component_saved_searches': [{
                        "name": "components",
                        "parameters": {}
                    }],
                    'relation_saved_searches': [],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }

        self.load_check(config)
        self.check.status.data['http://localhost:8089token'] = "dsvljbfovjsdvkj"
        self.check.status.persist("splunk_topology")

        def _mocked_token_auth_session(*args):
            raise TokenExpiredException("Current in use authentication token is expired. Please provide a valid "
                                        "token in the YAML and restart the Agent")

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_token_auth_session': _mocked_token_auth_session
        })

        msg = "Current in use authentication token is expired. Please provide a valid token in the YAML and restart" \
              " the Agent"
        # Invalid token should throw a service check with proper message
        self.assertEquals(self.service_checks[0]['status'], 2, msg)

        # clear the in memory token
        self.check.status.data.clear()
        self.check.status.persist("splunk_topology")
