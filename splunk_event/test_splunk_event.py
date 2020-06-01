# stdlib
import json
import os

from utils.splunk.splunk import time_to_seconds
from tests.checks.common import AgentCheckTest, Fixtures
from checks import CheckException, FinalizeException, TokenExpiredException

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), 'ci')

def _mocked_saved_searches(*args, **kwargs):
    return []

def _mocked_finalize_sid_none(*args, **kwargs):
    return None

def _mocked_auth_session(instance_key):
    return

class TestSplunkErrorResponse(AgentCheckTest):
    """
    Splunk event check should handle a FATAL message response
    """
    CHECK_NAME = 'splunk_event'

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
                    'saved_searches': [{
                        "name": "error",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        thrown = False
        try:
            self.run_check(config, mocks={
                '_auth_session': _mocked_auth_session,
                '_dispatch_saved_search': _mocked_dispatch_saved_search,
                '_saved_searches': _mocked_saved_searches
            })
        except CheckException:
            thrown = True
        self.assertTrue(thrown, "Retrieving FATAL message from Splunk should throw.")

        self.assertEquals(len(self.service_checks), 2)
        self.assertEquals(self.service_checks[0]['status'], 1, "service check should have status AgentCheck.CRITICAL")
        self.assertEquals(self.service_checks[1]['status'], 2, "service check should have status AgentCheck.CRITICAL")


class TestSplunkEmptyEvents(AgentCheckTest):
    """
    Splunk event check should process empty response correctly
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {}
                    }]
                }
            ]
        }
        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_minimal_search,
            '_saved_searches': _mocked_saved_searches
        })
        current_check_events = self.check.get_events()
        self.assertEqual(len(current_check_events), 0)

class TestSplunkMinimalEvents(AgentCheckTest):
    """
    Splunk event check should process minimal response correctly
    """
    CHECK_NAME = 'splunk_event'

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
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_minimal_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.events), 2)
        self.assertEqual(self.events[0], {
            'event_type': None,
            'tags': [],
            'timestamp': 1488974400.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })
        self.assertEqual(self.events[1], {
            'event_type': None,
            'tags': [],
            'timestamp': 1488974400.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })

    def test_checks_backward_compatibility(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_minimal_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.events), 2)
        self.assertEqual(self.events[0], {
            'event_type': None,
            'tags': [],
            'timestamp': 1488974400.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })
        self.assertEqual(self.events[1], {
            'event_type': None,
            'tags': [],
            'timestamp': 1488974400.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })

    def test_checks_backward_compatibility_with_new_conf(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_minimal_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.events), 2)
        self.assertEqual(self.events[0], {
            'event_type': None,
            'tags': [],
            'timestamp': 1488974400.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })
        self.assertEqual(self.events[1], {
            'event_type': None,
            'tags': [],
            'timestamp': 1488974400.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })


    def test_not_dispatch_sids_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001/',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "minimal_events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }
        instance = config.get('instances')[0]
        persist_status_key = instance.get('url') + "minimal_events"

        # Run the check first time and get the persistent status data
        self.run_check(config, mocks={
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search
        })

        first_persistent_data = self.check.status.data.get(persist_status_key)

        # Run the check 2nd time and get the persistent status data
        self.run_check(config, mocks={
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_finalize_sid': _mocked_finalize_sid_none
        }, force_reload=True)

        second_persistent_data = self.check.status.data.get(persist_status_key)
        # The second run_check will finalize the previous saved search ids and create a new one,
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
                '_dispatch_saved_search': _mocked_dispatch_saved_search,
                '_finalize_sid': _mocked_finalize_sid_exception
            }, force_reload=True)
        except CheckException:
            thrown = True

        self.assertTrue(thrown)

        # make sure the data still persists after exception raised
        self.assertIsNotNone(self.check.status.data.get(persist_status_key))

        # tear down the persistent data
        self.tear_down(instance.get('url'), "minimal_events")


class TestSplunkPartiallyIncompleteEvents(AgentCheckTest):
    """
    Splunk event check should continue processing even when some events are not complete
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_partially_incomplete_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.events), 1)
        self.assertEqual(self.events[0], {
            'event_type': None,
            'tags': [],
            'timestamp': 1488974400.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })

        self.assertEqual(len(self.service_checks), 1)
        self.assertEquals(self.service_checks[0]['status'], 1, "service check should have status AgentCheck.WARNING")
        self.assertEquals(self.service_checks[0]['message'],
                          "1 telemetry records failed to process when running saved search 'events'")



class TestSplunkFullEvents(AgentCheckTest):
    """
    Splunk event check should process full response correctly
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {}
                    }],
                    'tags': ["checktag:checktagvalue"]
                }
            ]
        }

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_full_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.events), 2)
        self.assertEqual(self.events[0], {
            'event_type': "some_type",
            'timestamp': 1488997796.0,
            'msg_title': "some_title",
            'msg_text': "some_text",
            'source_type_name': 'unknown-too_small',
            'tags': [
                'from:grey',
                "full_formatted_message:Alarm 'Virtual machine CPU usage' on SWNC7R049 changed from Gray to Green",
                "alarm_name:Virtual machine CPU usage",
                "to:green",
                "host:172.17.0.1",
                "key:19964908",
                "VMName:SWNC7R049",
                "checktag:checktagvalue"
            ]
        })

        self.assertEqual(self.events[1], {
            'event_type': "some_type",
            'timestamp': 1488997797.0,
            'msg_title': "some_title",
            'msg_text': "some_text",
            'source_type_name': 'unknown-too_small',
            'tags': [
                'from:grey',
                "full_formatted_message:Alarm 'Virtual machine memory usage' on SWNC7R049 changed from Gray to Green",
                "alarm_name:Virtual machine memory usage",
                "to:green",
                "host:172.17.0.1",
                "key:19964909",
                "VMName:SWNC7R049",
                "checktag:checktagvalue"
            ]
        })


class TestSplunkEarliestTimeAndDuplicates(AgentCheckTest):
    """
    Splunk event check should poll batches responses
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "poll",
                        "parameters": {},
                        "batch_size": 2
                    }],
                    'tags': ["checktag:checktagvalue"]
                }
            ]
        }

        # Used to validate which searches have been executed
        test_data = {
            "expected_searches": ["poll"],
            "sid": "",
            "time": 0,
            "earliest_time": "",
            "throw": False
        }

        def _mocked_current_time_seconds():
            return test_data["time"]

        def _mocked_polling_search(*args, **kwargs):
            sid = args[0]
            count = args[1].batch_size
            return json.loads(Fixtures.read_file("batch_%s_seq_%s.json" % (sid, count), sdk_dir=FIXTURE_DIR))

        def _mocked_dispatch_saved_search_dispatch(*args, **kwargs):
            if test_data["throw"]:
                raise CheckException("Is broke it")
            earliest_time = args[5]['dispatch.earliest_time']
            if test_data["earliest_time"] != "":
                self.assertEquals(earliest_time, test_data["earliest_time"])

            ignore_saved_search_flag = args[4]
            # make sure the ignore search flag is always false
            self.assertFalse(ignore_saved_search_flag)

            return test_data["sid"]

        test_mocks = {
            '_auth_session': _mocked_auth_session,
            '_dispatch': _mocked_dispatch_saved_search_dispatch,
            '_search': _mocked_polling_search,
            '_current_time_seconds': _mocked_current_time_seconds,
            '_saved_searches': _mocked_saved_searches,
            '_finalize_sid': _mocked_finalize_sid_none
        }

        # Initial run
        test_data["sid"] = "poll"
        test_data["time"] = time_to_seconds("2017-03-08T18:29:59.000000+0000")
        self.run_check(config, mocks=test_mocks)
        self.assertEqual(len(self.events), 4)
        self.assertEqual([e['event_type'] for e in self.events], ["0_1", "0_2", "1_1", "1_2"])

        # respect earliest_time
        test_data["sid"] = "poll1"
        test_data["earliest_time"] = '2017-03-08T18:30:00.000000+0000'
        self.run_check(config, mocks=test_mocks)
        self.assertEqual(len(self.events), 1)
        self.assertEqual([e['event_type'] for e in self.events], ["2_1"])

        # Throw exception during search
        test_data["throw"] = True
        thrown = False
        try:
            self.run_check(config, mocks=test_mocks)
        except CheckException:
            thrown = True
        self.assertTrue(thrown, "Expect thrown to be done from the mocked search")
        self.assertEquals(self.service_checks[1]['status'], 2, "service check should have status AgentCheck.CRITICAL")


class TestSplunkDelayFirstTime(AgentCheckTest):
    """
    Splunk event check should only start polling after the specified time
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {'default_initial_delay_seconds': 60},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        # Used to validate which searches have been executed
        test_data = {
            "time": 1,
        }

        def _mocked_current_time_seconds():
            return test_data["time"]

        mocks = {
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_minimal_search,
            '_current_time_seconds': _mocked_current_time_seconds,
            '_saved_searches': _mocked_saved_searches
        }

        # Initial run
        self.run_check(config, mocks=mocks)
        self.assertEqual(len(self.events), 0)

        # Not polling yet
        test_data["time"] = 30
        self.run_check(config, mocks=mocks)
        self.assertEqual(len(self.events), 0)

        # Start polling
        test_data["time"] = 62
        self.run_check(config, mocks=mocks)
        self.assertEqual(len(self.events), 2)


class TestSplunkDeduplicateEventsInTheSameRun(AgentCheckTest):
    """
    Splunk event check should deduplicate events
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {"default_batch_size": 2},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "duplicates",
                        "parameters": {}
                    }],
                    'tags': ["checktag:checktagvalue"]
                }
            ]
        }

        # Used to validate which searches have been executed
        test_data = {
            "expected_searches": ["duplicates"],
            "sid": "",
            "time": 0,
        }

        def _mocked_current_time_seconds():
            return test_data["time"]

        def _mocked_dup_search(*args, **kwargs):
            sid = args[0]
            count = args[1].batch_size
            return json.loads(Fixtures.read_file("batch_%s_seq_%s.json" % (sid, count), sdk_dir=FIXTURE_DIR))

        def _mocked_dispatch_saved_search_dispatch(*args, **kwargs):
            return test_data["sid"]

        test_mocks = {
            '_auth_session': _mocked_auth_session,
            '_dispatch': _mocked_dispatch_saved_search_dispatch,
            '_search': _mocked_dup_search,
            '_current_time_seconds': _mocked_current_time_seconds,
            '_saved_searches': _mocked_saved_searches
        }

        # Inital run
        test_data["sid"] = "no_dup"
        test_data["time"] = 1
        self.run_check(config, mocks=test_mocks)
        self.assertEqual(len(self.events), 2)
        self.assertEqual([e['event_type'] for e in self.events], ["1", "2"])


class TestSplunkContinueAfterRestart(AgentCheckTest):
    """
    Splunk event check should continue where it left off after restart
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {
                'default_max_restart_history_seconds': 86400,
                'default_max_query_time_range': 3600
            },
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "empty",
                        "parameters": {},
                        'max_restart_history_seconds': 86400,
                        'max_query_time_range': 3600
                    }],
                    'tags': ["checktag:checktagvalue"]
                }
            ]
        }

        # Used to validate which searches have been executed
        test_data = {
            "time": 0,
            "earliest_time": "",
            "latest_time": None
        }

        def _mocked_current_time_seconds():
            return test_data["time"]

        def _mocked_dispatch_saved_search_dispatch(*args, **kwargs):
            earliest_time = args[5]['dispatch.earliest_time']
            if test_data["earliest_time"] != "":
                self.assertEquals(earliest_time, test_data["earliest_time"])

            ignore_saved_search_flag = args[4]
            # make sure the ignore search flag is always false
            self.assertFalse(ignore_saved_search_flag)

            if test_data["latest_time"] is None:
                self.assertTrue('dispatch.latest_time' not in args[5])
            elif test_data["latest_time"] != "":
                self.assertEquals(args[5]['dispatch.latest_time'], test_data["latest_time"])

            return "empty"

        test_mocks = {
            '_auth_session': _mocked_auth_session,
            '_dispatch': _mocked_dispatch_saved_search_dispatch,
            '_search': _mocked_search,
            '_current_time_seconds': _mocked_current_time_seconds,
            '_saved_searches': _mocked_saved_searches,
            '_finalize_sid': _mocked_finalize_sid_none
        }

        # Initial run with initial time
        test_data["time"] = time_to_seconds('2017-03-08T00:00:00.000000+0000')
        test_data["earliest_time"] = '2017-03-08T00:00:00.000000+0000'
        test_data["latest_time"] = None
        self.run_check(config, mocks=test_mocks)
        self.assertEqual(len(self.events), 0)

        # Restart check and recover data
        test_data["time"] = time_to_seconds('2017-03-08T01:00:05.000000+0000')
        for slice_num in range(0, 12):
            test_data["earliest_time"] = '2017-03-08T00:%s:01.000000+0000' % (str(slice_num * 5).zfill(2))
            test_data["latest_time"] = '2017-03-08T00:%s:01.000000+0000' % (str((slice_num + 1) * 5).zfill(2))
            if slice_num == 11:
                test_data["latest_time"] = '2017-03-08T01:00:01.000000+0000'
            self.run_check(config, mocks=test_mocks, force_reload=slice_num == 0)
            self.assertTrue(self.continue_after_commit, "As long as we are not done with history, the check should continue")

        # Now continue with real-time polling (earliest time taken from last event or last restart chunk)
        test_data["earliest_time"] = '2017-03-08T01:00:01.000000+0000'
        test_data["latest_time"] = None
        self.run_check(config, mocks=test_mocks)
        self.assertFalse(self.continue_after_commit, "As long as we are not done with history, the check should continue")


class TestSplunkQueryInitialHistory(AgentCheckTest):
    """
    Splunk event check should continue where it left off after restart
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {
                'default_initial_history_time_seconds': 86400,
                'default_max_query_chunk_seconds': 3600
            },
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "empty",
                        "parameters": {},
                        'max_initial_history_seconds': 86400,
                        'max_query_chunk_seconds': 3600
                    }],
                    'tags': ["checktag:checktagvalue"]
                }
            ]
        }

        # Used to validate which searches have been executed
        test_data = {
            "time": 0,
            "earliest_time": "",
            "latest_time": ""
        }

        def _mocked_current_time_seconds():
            return test_data["time"]

        def _mocked_dispatch_saved_search_dispatch(*args, **kwargs):
            earliest_time = args[5]['dispatch.earliest_time']
            if test_data["earliest_time"] != "":
                self.assertEquals(earliest_time, test_data["earliest_time"])

            if test_data["latest_time"] is None:
                self.assertTrue('dispatch.latest_time' not in args[5])
            elif test_data["latest_time"] != "":
                self.assertEquals(args[5]['dispatch.latest_time'], test_data["latest_time"])

            return "events"

        test_mocks = {
            '_auth_session': _mocked_auth_session,
            '_dispatch': _mocked_dispatch_saved_search_dispatch,
            '_search': _mocked_minimal_search,
            '_current_time_seconds': _mocked_current_time_seconds,
            '_saved_searches': _mocked_saved_searches,
            '_finalize_sid': _mocked_finalize_sid_none
        }

        test_data["time"] = time_to_seconds('2017-03-09T00:00:00.000000+0000')

        # Gather initial data
        for slice_num in range(0, 23):
            test_data["earliest_time"] = '2017-03-08T%s:00:00.000000+0000' % (str(slice_num).zfill(2))
            test_data["latest_time"] = '2017-03-08T%s:00:00.000000+0000' % (str(slice_num + 1).zfill(2))
            self.run_check(config, mocks=test_mocks)
            self.assertTrue(self.continue_after_commit, "As long as we are not done with history, the check should continue")

        # Now continue with real-time polling (earliest time taken from last event)
        test_data["earliest_time"] = '2017-03-08T23:00:00.000000+0000'
        test_data["latest_time"] = None
        self.run_check(config, mocks=test_mocks)
        self.assertEqual(len(self.events), 2)
        self.assertFalse(self.continue_after_commit, "As long as we are not done with history, the check should continue")


class TestSplunkMaxRestartTime(AgentCheckTest):
    """
    Splunk event check should use the max restart time parameter
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {
                'default_restart_history_time_seconds': 3600,
                'default_max_query_chunk_seconds': 3600
            },
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "empty",
                        "parameters": {},
                        'max_restart_history_seconds': 3600,
                        'max_query_chunk_seconds': 3600
                    }],
                    'tags': ["checktag:checktagvalue"]
                }
            ]
        }

        # Used to validate which searches have been executed
        test_data = {
            "time": 0,
            "earliest_time": ""
        }

        def _mocked_current_time_seconds():
            return test_data["time"]

        def _mocked_dispatch_saved_search_dispatch(*args, **kwargs):
            earliest_time = args[5]['dispatch.earliest_time']
            if test_data["earliest_time"] != "":
                self.assertEquals(earliest_time, test_data["earliest_time"])

            return "empty"

        test_mocks = {
            '_auth_session': _mocked_auth_session,
            '_dispatch': _mocked_dispatch_saved_search_dispatch,
            '_search': _mocked_search,
            '_current_time_seconds': _mocked_current_time_seconds,
            '_saved_searches': _mocked_saved_searches,
            '_finalize_sid': _mocked_finalize_sid_none
        }

        # Initial run with initial time
        test_data["time"] = time_to_seconds('2017-03-08T00:00:00.000000+0000')
        test_data["earliest_time"] = '2017-03-08T00:00:00.000000+0000'
        self.run_check(config, mocks=test_mocks)
        self.assertEqual(len(self.events), 0)

        # Restart check and recover data, taking into account the max restart history
        test_data["time"] = time_to_seconds('2017-03-08T12:00:00.000000+0000')
        test_data["earliest_time"] = '2017-03-08T11:00:00.000000+0000'
        test_data["latest_time"] = '2017-03-08T11:00:00.000000+0000'
        self.run_check(config, mocks=test_mocks, force_reload=True)


class TestSplunkKeepTimeOnFailure(AgentCheckTest):
    """
    Splunk event check should keep the same start time when commit fails.
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {
            },
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {},
                    }],
                    'tags': ["checktag:checktagvalue"]
                }
            ]
        }

        # Used to validate which searches have been executed
        test_data = {
            "time": 0,
            "earliest_time": ""
        }

        def _mocked_current_time_seconds():
            return test_data["time"]

        def _mocked_dispatch_saved_search_dispatch(*args, **kwargs):
            earliest_time = args[5]['dispatch.earliest_time']
            if test_data["earliest_time"] != "":
                self.assertEquals(earliest_time, test_data["earliest_time"])

            return "events"

        test_mocks = {
            '_auth_session': _mocked_auth_session,
            '_dispatch': _mocked_dispatch_saved_search_dispatch,
            '_search': _mocked_minimal_search,
            '_current_time_seconds': _mocked_current_time_seconds,
            '_saved_searches': _mocked_saved_searches,
            '_finalize_sid': _mocked_finalize_sid_none
        }

        self.collect_ok = False

        # Run the check, collect will fail
        test_data["time"] = time_to_seconds('2017-03-08T11:00:00.000000+0000')
        test_data["earliest_time"] = '2017-03-08T11:00:00.000000+0000'
        self.run_check(config, mocks=test_mocks)
        self.assertEqual(len(self.events), 2)

        # Make sure we keep the same start time
        self.run_check(config, mocks=test_mocks)


class TestSplunkAdvanceTimeOnSuccess(AgentCheckTest):
    """
    Splunk event check should advance the start time when commit succeeds
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {
            },
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {},
                    }],
                    'tags': ["checktag:checktagvalue"]
                }
            ]
        }

        # Used to validate which searches have been executed
        test_data = {
            "time": 0,
            "earliest_time": ""
        }

        def _mocked_current_time_seconds():
            return test_data["time"]

        def _mocked_dispatch_saved_search_dispatch(*args, **kwargs):
            earliest_time = args[5]['dispatch.earliest_time']
            if test_data["earliest_time"] != "":
                self.assertEquals(earliest_time, test_data["earliest_time"])

            return "events"

        test_mocks = {
            '_auth_session': _mocked_auth_session,
            '_dispatch': _mocked_dispatch_saved_search_dispatch,
            '_search': _mocked_minimal_search,
            '_current_time_seconds': _mocked_current_time_seconds,
            '_saved_searches': _mocked_saved_searches,
            '_finalize_sid': _mocked_finalize_sid_none
        }

        # Run the check, collect will fail
        test_data["time"] = time_to_seconds('2017-03-08T11:00:00.000000+0000')
        test_data["earliest_time"] = '2017-03-08T11:00:00.000000+0000'
        self.run_check(config, mocks=test_mocks)
        self.assertEqual(len(self.events), 2)

        # Make sure we advance the start time
        test_data["earliest_time"] = '2017-03-08T12:00:01.000000+0000'
        self.run_check(config, mocks=test_mocks)


class TestSplunkWildcardSearches(AgentCheckTest):
    """
    Splunk event check should process minimal response correctly
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "match": "even*",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        data = {
            'saved_searches': []
        }

        def _mocked_saved_searches(*args, **kwargs):
            return data['saved_searches']

        data['saved_searches'] = ["events", "blaat"]
        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_minimal_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.check.instance_data['http://localhost:13001'].saved_searches.searches), 1)
        self.assertEqual(len(self.events), 2)

        data['saved_searches'] = []
        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_minimal_search,
            '_saved_searches': _mocked_saved_searches
        })
        self.assertEqual(len(self.check.instance_data['http://localhost:13001'].saved_searches.searches), 0)
        self.assertEqual(len(self.events), 0)


class TestSplunkSavedSearchesError(AgentCheckTest):
    """
    Splunk event check should have a service check failure when getting an exception from saved searches
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "match": "even*",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        def _mocked_saved_searches(*args, **kwargs):
            raise Exception("Boom")

        thrown = False
        try:
            self.run_check(config, mocks={
                '_auth_session': _mocked_auth_session,
                '_saved_searches': _mocked_saved_searches
            })
        except CheckException:
            thrown = True
        self.assertTrue(thrown, "Retrieving FATAL message from Splunk should throw.")
        self.assertEquals(self.service_checks[0]['status'], 2, "service check should have status AgentCheck.CRITICAL")


class TestSplunkSavedSearchesIgnoreError(AgentCheckTest):
    """
    Splunk event check should ignore exception when getting an exception from saved searches
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'ignore_saved_search_errors': True,
                    'saved_searches': [{
                        "match": "metric*",
                        "parameters": {}
                    }],
                    'tags': []
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


def _mocked_dispatch_saved_search(*args, **kwargs):
    # Sid is equal to search name
    return args[1].name

def _mocked_search(*args, **kwargs):
    # sid is set to saved search name
    sid = args[0]
    return [json.loads(Fixtures.read_file("%s.json" % sid, sdk_dir=FIXTURE_DIR))]

def _mocked_minimal_search(*args, **kwargs):
    # sid is set to saved search name
    sid = args[0]
    return [json.loads(Fixtures.read_file("minimal_%s.json" % sid, sdk_dir=FIXTURE_DIR))]

def _mocked_partially_incomplete_search(*args, **kwargs):
    # sid is set to saved search name
    sid = args[0]
    return [json.loads(Fixtures.read_file("partially_incomplete_%s.json" % sid, sdk_dir=FIXTURE_DIR))]

def _mocked_full_search(*args, **kwargs):
    # sid is set to saved search name
    sid = args[0]
    return [json.loads(Fixtures.read_file("full_%s.json" % sid, sdk_dir=FIXTURE_DIR))]

def _mocked_identification_fields_search(*args, **kwargs):
    # sid is set to saved search name
    sid = args[0]
    return [json.loads(Fixtures.read_file("identification_fields_%s.json" % sid, sdk_dir=FIXTURE_DIR))]

class TestSplunkEventRespectParallelDispatches(AgentCheckTest):
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        saved_searches_parallel = 2

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches_parallel': saved_searches_parallel,
                    'saved_searches': [
                        {"name": "savedsearch1", "parameters": {}},
                        {"name": "savedsearch2", "parameters": {}},
                        {"name": "savedsearch3", "parameters": {}},
                        {"name": "savedsearch4", "parameters": {}},
                        {"name": "savedsearch5", "parameters": {}}
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
            '_auth_session': _mocked_auth_session,
            '_dispatch_and_await_search': _mock_dispatch_and_await_search,
            '_saved_searches': _mocked_saved_searches
        })


class TestSplunkSelectiveFieldsForIdentification(AgentCheckTest):
    """
    Splunk event check should process events where the unique identifier is set to a selective number of fields
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "selective_events",
                        "parameters": {},
                        "unique_key_fields": ["uid1", "uid2"]
                    }],
                    'tags': []
                }
            ]
        }

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_identification_fields_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.events), 2)
        self.assertEqual(self.events[0], {
            'event_type': u"some_type",
            'tags': [u"uid2:1", u"uid1:uid"],
            'timestamp': 4100437796.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })
        self.assertEqual(self.events[1], {
            'event_type': u"some_type",
            'tags': [u"uid2:2", u"uid1:uid"],
            'timestamp': 4100437796.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })

        # shouldn't resend events
        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_identification_fields_search,
            '_saved_searches': _mocked_saved_searches,
            '_finalize_sid': _mocked_finalize_sid_none
        })
        self.assertEqual(len(self.events), 0)


class TestSplunkAllFieldsForIdentification(AgentCheckTest):
    """
    Splunk event check should process events where the unique identifier is set to all fields in a record
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "all_events",
                        "parameters": {},
                        "unique_key_fields": []
                    }],
                    'tags': []
                }
            ]
        }

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_identification_fields_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.events), 2)
        self.assertEqual(self.events[0], {
            'event_type': u"some_type",
            'tags': [u"value:1"],
            'timestamp': 4100437796.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })
        self.assertEqual(self.events[1], {
            'event_type': u"some_type",
            'tags': [u"value:2"],
            'timestamp': 4100437796.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })

        # shouldn't resend events
        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_identification_fields_search,
            '_saved_searches': _mocked_saved_searches,
            '_finalize_sid': _mocked_finalize_sid_none
        })
        self.assertEqual(len(self.events), 0)


class TestSplunkEventIndividualDispatchFailures(AgentCheckTest):
    """
    Splunk metric check shouldn't fail if individual failures occur when dispatching Splunk searches
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "match": ".*events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        data = {
            'saved_searches': []
        }

        def _mocked_saved_searches(*args, **kwargs):
            return data['saved_searches']

        data['saved_searches'] = ["minimal_events", "full_events"]

        def _mocked_dispatch_saved_search(*args, **kwargs):
            name = args[1].name
            if name == "full_events":
                raise Exception("BOOM")
            else:
                return name

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            "_saved_searches": _mocked_saved_searches,
            "_dispatch_saved_search": _mocked_dispatch_saved_search,
            "_search": _mocked_search
        })

        self.assertEqual(len(self.events), 2)

        self.assertEqual(len(self.service_checks), 1)
        self.assertEquals(self.service_checks[0]['status'], 1, "service check should have status AgentCheck.WARNING")
        self.assertEquals(self.service_checks[0]['message'], "Failed to dispatch saved search 'full_events' due to: BOOM")


class TestSplunkEventIndividualSearchFailures(AgentCheckTest):
    """
    Splunk metric check shouldn't fail if individual failures occur when executing Splunk searches
    """
    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "match": ".*events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        data = {
            'saved_searches': []
        }

        def _mocked_saved_searches(*args, **kwargs):
            return data['saved_searches']

        data['saved_searches'] = ["minimal_events", "full_events"]

        def _mocked_failing_search(*args, **kwargs):
            sid = args[1].name
            if sid == "full_events":
                raise Exception("BOOM")
            else:
                return _mocked_search(*args, **kwargs)

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            "_saved_searches": _mocked_saved_searches,
            "_dispatch_saved_search": _mocked_dispatch_saved_search,
            "_search": _mocked_failing_search
        })

        self.assertEqual(len(self.events), 2)

        self.assertEqual(len(self.service_checks), 1)
        self.assertEquals(self.service_checks[0]['status'], 1, "service check should have status AgentCheck.WARNING")
        self.assertEquals(self.service_checks[0]['message'], "Failed to execute dispatched search 'full_events' with id full_events due to: BOOM")


class TestSplunkEventSearchFullFailure(AgentCheckTest):
    """
    Splunk metric check should fail when all saved searches fail
    """

    CHECK_NAME = 'splunk_event'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "match": ".*events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        data = {
            'saved_searches': []
        }

        def _mocked_saved_searches(*args, **kwargs):
            return data['saved_searches']

        data['saved_searches'] = ["minimal_events", "full_events"]

        def _mocked_dispatch_saved_search(*args, **kwargs):
            raise Exception("BOOM")

        thrown = False

        try:
            self.run_check(config, mocks={
                '_auth_session': _mocked_auth_session,
                "_saved_searches": _mocked_saved_searches,
                "_dispatch_saved_search": _mocked_dispatch_saved_search
            })
        except Exception:
            thrown = True

        self.assertTrue(thrown, "All saved searches should fail and an exception should've been thrown")


class TestSplunkDefaults(AgentCheckTest):
    CHECK_NAME = 'splunk_event'

    def test_default_parameters(self):
        """
        when no default parameters are provided, the code should provide the parameters
        """
        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "events"
                    }],
                    'tags': ["checktag:checktagvalue"]
                }
            ]
        }
        expected_default_parameters = {'dispatch.now': True, 'force_dispatch': True}

        def _mocked_auth_session_to_check_instance_config(instance):
            for saved_search in instance.saved_searches.searches:
                self.assertEqual(saved_search.parameters, expected_default_parameters, msg="Unexpected default parameters for saved search: %s" % saved_search.name)
            return "sessionKey1"

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session_to_check_instance_config,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_full_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.events), 2)


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
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "events"
                    }],
                    'tags': ["checktag:checktagvalue"]
                }
            ]
        }
        expected_default_parameters = {'respect': 'me'}

        def _mocked_auth_session_to_check_instance_config(instance):
            for saved_search in instance.saved_searches.searches:
                self.assertEqual(saved_search.parameters, expected_default_parameters, msg="Unexpected non-default parameters for saved search: %s" % saved_search.name)
            return "sessionKey1"

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session_to_check_instance_config,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_full_search,
            '_saved_searches': _mocked_saved_searches
        })
        self.assertEqual(len(self.events), 2)


    def test_overwrite_default_parameters(self):
        """
        when default parameters are overwritten, the code should respect them.
        """
        config = {
            'init_config': {
                'init_config': {
                    'default_parameters': {
                        'default_should': 'be ignored'
                    }
                },
            },
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'basic_auth': {
                            'username': "admin",
                            'password': "admin"
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {
                            "respect": "me"
                        }
                    }],
                    'tags': ["checktag:checktagvalue"]
                }
            ]
        }

        expected_default_parameters = {'respect': 'me'}

        def _mocked_auth_session_to_check_instance_config(instance):
            for saved_search in instance.saved_searches.searches:
                self.assertEqual(saved_search.parameters, expected_default_parameters, msg="Unexpected overwritten parameters for saved search: %s" % saved_search.name)
            return "sessionKey1"

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session_to_check_instance_config,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_full_search,
            '_saved_searches': _mocked_saved_searches
        })
        self.assertEqual(len(self.events), 2)


class TestSplunkEventsWithTokenAuth(AgentCheckTest):
    """
    Splunk event check should process minimal response correctly
    """
    CHECK_NAME = 'splunk_event'

    def test_checks_with_valid_token(self):
        """
            Splunk event check should work with valid initial token
        """
        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'token_auth': {
                            'name': "admin",
                            'initial_token': "dsfdgfhgjhkjuyr567uhfe345ythu7y6tre456sdx",
                            'audience': "search",
                            'renewal_days': 10
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        def _mocked_token_auth_session(*args):
            return None

        self.run_check(config, mocks={
            '_token_auth_session': _mocked_token_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_minimal_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.events), 2)
        self.assertEqual(self.events[0], {
            'event_type': None,
            'tags': [],
            'timestamp': 1488974400.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })
        self.assertEqual(self.events[1], {
            'event_type': None,
            'tags': [],
            'timestamp': 1488974400.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })
        # clear the in memory token
        self.check.status.data.clear()
        self.check.status.persist("splunk_event")

    def test_checks_with_invalid_token(self):
        """
            Splunk check should not work with invalid initial token and stop the check
        """
        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'token_auth': {
                            'name': "admin",
                            'initial_token': "dsfdgfhgjhkjuyr567uhfe345ythu7y6tre456sdx",
                            'audience': "search",
                            'renewal_days': 10
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        def _mocked_token_auth_session(*args):
            raise TokenExpiredException("Current in use authentication token is expired. Please provide a valid "
                                        "token in the YAML and restart the Agent")

        self.run_check(config, mocks={
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_minimal_search,
            '_saved_searches': _mocked_saved_searches,
            '_token_auth_session': _mocked_token_auth_session
        })

        msg = "Current in use authentication token is expired. Please provide a valid token in the YAML and restart " \
              "the Agent"
        # Invalid token should throw a service check with proper message
        self.assertEquals(self.service_checks[0]['status'], 2, msg)
        # clear the in memory token
        self.check.status.data.clear()
        self.check.status.persist("splunk_event")

    def test_check_audience_param_not_set(self):
        """
            Splunk event check should fail and raise exception when audience param is not set
        """

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'token_auth': {
                            'name': "admin",
                            'initial_token': "dsfdgfhgjhkjuyr567uhfe345ythu7y6tre456sdx",
                            'renewal_days': 10
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }
        # This is done to avoid going in the commit_succeeded call after the check runs
        self.collect_ok = False

        check = False

        try:
            self.run_check(config, mocks={
                '_dispatch_saved_search': _mocked_dispatch_saved_search,
                '_search': _mocked_search,
                '_saved_searches': _mocked_saved_searches,
            })
        except CheckException:
            check = True

        self.assertTrue(check, msg='Splunk event instance missing "authentication.token_auth.audience" value')

    def test_check_name_param_not_set(self):
        """
            Splunk event check should fail and raise exception when audience param is not set
        """
        self.maxDiff = None
        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'token_auth': {
                            'initial_token': "dsfdgfhgjhkjuyr567uhfe345ythu7y6tre456sdx",
                            'audience': "search",
                            'renewal_days': 10
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }
        # This is done to avoid going in the commit_succeeded call after the check runs
        self.collect_ok = False

        check = False

        try:
            self.run_check(config, mocks={
                '_dispatch_saved_search': _mocked_dispatch_saved_search,
                '_search': _mocked_search,
                '_saved_searches': _mocked_saved_searches,
            })
        except CheckException:
            check = True

        self.assertTrue(check, msg='Splunk event instance missing "authentication.token_auth.audience" value')

    def test_check_token_auth_preferred_over_basic_auth(self):
        """
            Splunk event check should prefer Token based authentication over Basic auth mechanism
        """
        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'token_auth': {
                            'name': "admin",
                            'initial_token': "dsfdgfhgjhkjuyr567uhfe345ythu7y6tre456sdx",
                            'audience': "search",
                            'renewal_days': 10
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        def _mocked_token_auth_session(*args):
            return None

        self.run_check(config, mocks={
            '_token_auth_session': _mocked_token_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_minimal_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.events), 2)
        self.assertEqual(self.events[0], {
            'event_type': None,
            'tags': [],
            'timestamp': 1488974400.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })
        self.assertEqual(self.events[1], {
            'event_type': None,
            'tags': [],
            'timestamp': 1488974400.0,
            'msg_title': None,
            'msg_text': None,
            'source_type_name': None
        })
        # clear the in memory token
        self.check.status.data.clear()
        self.check.status.persist("splunk_event")

    def test_check_memory_token_expired(self):
        """
            Splunk event check should fail when memory token is expired itself.
        """
        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'authentication': {
                        'token_auth': {
                            'name': "admin",
                            'initial_token': "dsfdgfhgjhkjuyr567uhfe345ythu7y6tre456sdx",
                            'audience': "search",
                            'renewal_days': 10
                        }
                    },
                    'saved_searches': [{
                        "name": "events",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        self.load_check(config)
        self.check.status.data.clear()
        self.check.status.data['http://localhost:13001token'] = "dsvljbfovjsdvkj"
        self.check.status.persist("splunk_event")

        def _mocked_token_auth_session(*args):
            raise TokenExpiredException("Current in use authentication token is expired. Please provide a valid "
                                        "token in the YAML and restart the Agent")

        self.run_check(config, mocks={
            '_token_auth_session': _mocked_token_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_minimal_search,
            '_saved_searches': _mocked_saved_searches
        })

        msg = "Current in use authentication token is expired. Please provide a valid token in the YAML and restart" \
              " the Agent"
        # Invalid token should throw a service check with proper message
        self.assertEquals(self.service_checks[0]['status'], 2, msg)
        # clear the in memory token
        self.check.status.data.clear()
        self.check.status.persist("splunk_event")
