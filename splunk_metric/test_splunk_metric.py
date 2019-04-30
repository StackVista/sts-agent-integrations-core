# stdlib
import json
import os

from utils.splunk.splunk import time_to_seconds
from tests.checks.common import AgentCheckTest, Fixtures
from checks import CheckException, FinalizeException

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), 'ci')


def _mocked_saved_searches(*args, **kwargs):
    return []


def _mocked_finalize_sid_none(*args, **kwargs):
    return None


def _mocked_dispatch_saved_search(*args, **kwargs):
    # Sid is equal to search name
    return args[1].name


def _mocked_search(*args, **kwargs):
    # sid is set to saved search name
    sid = args[0]
    return [json.loads(Fixtures.read_file("%s.json" % sid, sdk_dir=FIXTURE_DIR))]


def _mocked_auth_session(instance_config):
    return "sessionKey1"


class TestSplunkErrorResponse(AgentCheckTest):
    """
    Splunk metric check should handle a FATAL message response
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'username': "admin",
                    'password': "admin",
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
        self.assertEquals(self.service_checks[1]['status'], 2, "service check should have status AgentCheck.CRITICAL")


class TestSplunkMetric(AgentCheckTest):
    """
        Splunk metric check should handle already available search ids
    """
    CHECK_NAME = 'splunk_metric'

    def tear_down(self, url, qualifier):
        """
        Clear the persistent state from the system for next time
        """
        self.check.update_persistent_status(url, qualifier, None, 'clear')

    def test_not_dispatch_sids_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089/',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "minimal_metrics",
                        "parameters": {}
                    }],
                    'tags': ['mytag', 'mytag2']
                }
            ]
        }
        instance = config.get('instances')[0]
        persist_status_key = instance.get('url') + "minimal_metrics"

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
        # so we make sure sid is same both time for same saved search
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
        self.tear_down(instance.get('url'), "minimal_metrics")


class TestSplunkEmptyMetrics(AgentCheckTest):
    """
    Splunk metric check should process empty response correctly
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "empty",
                        "parameters": {}
                    }]
                }
            ]
        }
        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })
        current_check_metrics = self.check.get_metrics()
        self.assertEqual(len(current_check_metrics), 0)


class TestSplunkMinimalMetrics(AgentCheckTest):
    """
    Splunk metrics check should process minimal response correctly
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "minimal_metrics",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.metrics), 2)
        self.assertMetric(
            'metric_name',
            time=1488974400.0,
            value=1.0,
            tags=[])
        self.assertMetric(
            'metric_name',
            time=1488974400.0,
            value=2,
            tags=[])


class TestSplunkPartiallyIncompleteMetrics(AgentCheckTest):
    """
    Splunk metrics check should process continue when at least 1 datapoint was ok
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "partially_incomplete_metrics",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.metrics), 1)
        self.assertMetric(
            'metric_name',
            time=1488974400.0,
            value=1.0,
            tags=[])

        self.assertEqual(len(self.service_checks), 1)
        self.assertEquals(self.service_checks[0]['status'], 1, "service check should have status AgentCheck.WARNING")
        self.assertEquals(self.service_checks[0]['message'],
                          "1 telemetry records failed to process when running saved search 'partially_incomplete_metrics'")

class TestSplunkFullMetrics(AgentCheckTest):
    """
    Splunk metric check should process full response correctly
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "full_metrics",
                        "parameters": {}
                    }],
                    'tags': ["checktag:checktagvalue"]
                }
            ]
        }

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.metrics), 2)
        self.assertMetric(
            'metric_name',
            time=1488997796.0,
            value=1,
            tags=[
                'hostname:myhost',
                'some:tag',
                'checktag:checktagvalue'
            ])
        self.assertMetric(
            'metric_name',
            time=1488997797.0,
            value=1,
            tags=[
                'hostname:123',
                'some:123',
                'device_name:123',
                'checktag:checktagvalue'
            ])


class TestSplunkAlternativeFieldsMetrics(AgentCheckTest):
    """
    Splunk metrics check should be able to have configurable value fields
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "alternative_fields_metrics",
                        "metric_name_field": "mymetric",
                        "metric_value_field": "myvalue",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.metrics), 2)
        self.assertMetric(
            'metric_name',
            time=1488974400.0,
            value=1.0,
            tags=[])
        self.assertMetric(
            'metric_name',
            time=1488974400.0,
            value=2.0,
            tags=[])


class TestSplunkFixedMetricNAme(AgentCheckTest):
    """
    Splunk metrics check should be able to have a fixed check name
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "alternative_fields_metrics",
                        "metric_name": "custommetric",
                        "metric_value_field": "myvalue",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.metrics), 2)
        self.assertMetric(
            'custommetric',
            time=1488974400.0,
            value=1.0,
            tags=["mymetric:metric_name"])
        self.assertMetric(
            'custommetric',
            time=1488974400.0,
            value=2.0,
            tags=["mymetric:metric_name"])


class TestSplunkWarningOnMissingFields(AgentCheckTest):
    """
    Splunk metric check should produce a service check upon a missing value or metric name field
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:8089',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "incomplete_metrics",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEquals(self.service_checks[0]['status'], 1, "service check should have status AgentCheck.WARNING when fields are missing")


class TestSplunkSameDataMetrics(AgentCheckTest):
    """
    Splunk metrics check should process metrics with the same data
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "duplicate_metrics",
                        "parameters": {}
                    }],
                    'tags': []
                }
            ]
        }

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.metrics), 2)
        self.assertMetric(
            'metric_name',
            time=1488974400.0,
            value=1,
            tags=[])
        self.assertMetric(
            'metric_name',
            time=1488974400.0,
            value=1,
            tags=[])


class TestSplunkEarliestTimeAndDuplicates(AgentCheckTest):
    """
    Splunk metric check should poll batches responses
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
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
        self.assertEqual(len(self.metrics), 4)
        self.assertEqual([e[2] for e in self.metrics], [11, 12, 21, 22])

        # respect earliest_time
        test_data["sid"] = "poll1"
        test_data["earliest_time"] = '2017-03-08T18:29:59.000000+0000'
        self.run_check(config, mocks=test_mocks)
        self.assertEqual(len(self.metrics), 1)
        self.assertEqual([e[2] for e in self.metrics], [31])

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
    Splunk metric check should only start polling after the specified time
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {'default_initial_delay_seconds': 60},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "minimal_metrics",
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
            '_search': _mocked_search,
            '_current_time_seconds': _mocked_current_time_seconds,
            '_saved_searches': _mocked_saved_searches
        }

        # Initial run
        self.run_check(config, mocks=mocks)
        self.assertEqual(len(self.metrics), 0)

        # Not polling yet
        test_data["time"] = 30
        self.run_check(config, mocks=mocks)
        self.assertEqual(len(self.metrics), 0)

        # Start polling
        test_data["time"] = 62
        self.run_check(config, mocks=mocks)
        self.assertEqual(len(self.metrics), 2)


class TestSplunkContinueAfterRestart(AgentCheckTest):
    """
    Splunk metric check should continue where it left off after restart
    """
    CHECK_NAME = 'splunk_metric'

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
                    'username': "admin",
                    'password': "admin",
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
        self.assertEqual(len(self.metrics), 0)

        # Restart check and recover data
        test_data["time"] = time_to_seconds('2017-03-08T12:00:00.000000+0000')
        for slice_num in range(0, 11):
            test_data["earliest_time"] = '2017-03-08T%s:00:01.000000+0000' % (str(slice_num).zfill(2))
            test_data["latest_time"] = '2017-03-08T%s:00:01.000000+0000' % (str(slice_num + 1).zfill(2))
            self.run_check(config, mocks=test_mocks, force_reload=slice_num == 0)
            self.assertTrue(self.continue_after_commit, "As long as we are not done with history, the check should continue")

        # Now continue with real-time polling (earliest time taken from last event or last restart chunk)
        test_data["earliest_time"] = '2017-03-08T11:00:01.000000+0000'
        test_data["latest_time"] = None
        self.run_check(config, mocks=test_mocks)
        self.assertFalse(self.continue_after_commit, "As long as we are not done with history, the check should continue")


class TestSplunkQueryInitialHistory(AgentCheckTest):
    """
    Splunk metric check should continue where it left off after restart
    """
    CHECK_NAME = 'splunk_metric'

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
                    'username': "admin",
                    'password': "admin",
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

            return "minimal_metrics"

        test_mocks = {
            '_auth_session': _mocked_auth_session,
            '_dispatch': _mocked_dispatch_saved_search_dispatch,
            '_search': _mocked_search,
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
        self.assertEqual(len(self.metrics), 2)
        self.assertFalse(self.continue_after_commit, "As long as we are not done with history, the check should continue")


class TestSplunkMaxRestartTime(AgentCheckTest):
    """
    Splunk metric check should use the max restart time parameter
    """
    CHECK_NAME = 'splunk_metric'

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
                    'username': "admin",
                    'password': "admin",
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
        self.assertEqual(len(self.metrics), 0)

        # Restart check and recover data, taking into account the max restart history
        test_data["time"] = time_to_seconds('2017-03-08T12:00:00.000000+0000')
        test_data["earliest_time"] = '2017-03-08T11:00:00.000000+0000'
        test_data["latest_time"] = '2017-03-08T11:00:00.000000+0000'
        self.run_check(config, mocks=test_mocks, force_reload=True)


class TestSplunkKeepTimeOnFailure(AgentCheckTest):
    """
    Splunk metric check should keep the same start time when commit fails.
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {
            },
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "minimal_metrics",
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

            return "minimal_metrics"

        test_mocks = {
            '_auth_session': _mocked_auth_session,
            '_dispatch': _mocked_dispatch_saved_search_dispatch,
            '_search': _mocked_search,
            '_current_time_seconds': _mocked_current_time_seconds,
            '_saved_searches': _mocked_saved_searches,
            '_finalize_sid': _mocked_finalize_sid_none
        }

        self.collect_ok = False

        # Run the check, collect will fail
        test_data["time"] = time_to_seconds('2017-03-08T11:00:00.000000+0000')
        test_data["earliest_time"] = '2017-03-08T11:00:00.000000+0000'
        self.run_check(config, mocks=test_mocks)
        self.assertEqual(len(self.metrics), 2)

        # Make sure we keep the same start time
        self.run_check(config, mocks=test_mocks)


class TestSplunkAdvanceTimeOnSuccess(AgentCheckTest):
    """
    Splunk metric check should advance the start time when commit succeeds
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {
            },
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "minimal_metrics",
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

            return "minimal_metrics"

        test_mocks = {
            '_auth_session': _mocked_auth_session,
            '_dispatch': _mocked_dispatch_saved_search_dispatch,
            '_search': _mocked_search,
            '_current_time_seconds': _mocked_current_time_seconds,
            '_saved_searches': _mocked_saved_searches,
            '_finalize_sid': _mocked_finalize_sid_none
        }

        # Run the check, collect will fail
        test_data["time"] = time_to_seconds('2017-03-08T11:00:00.000000+0000')
        test_data["earliest_time"] = '2017-03-08T11:00:00.000000+0000'
        self.run_check(config, mocks=test_mocks)
        self.assertEqual(len(self.metrics), 2)

        # Make sure we advance the start time
        test_data["earliest_time"] = '2017-03-08T12:00:00.000000+0000'
        self.run_check(config, mocks=test_mocks)


class TestSplunkWildcardSearches(AgentCheckTest):
    """
    Splunk metric check should process minimal response correctly
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "match": "minimal_*",
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

        data['saved_searches'] = ["minimal_metrics", "blaat"]
        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.check.instance_data['http://localhost:13001'].saved_searches.searches), 1)
        self.assertEqual(len(self.metrics), 2)

        data['saved_searches'] = []
        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })
        self.assertEqual(len(self.check.instance_data['http://localhost:13001'].saved_searches.searches), 0)
        self.assertEqual(len(self.metrics), 0)


class TestSplunkSavedSearchesError(AgentCheckTest):
    """
    Splunk metric check should have a service check failure when getting an exception from saved searches
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
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
        self.assertTrue(thrown, "Retrieving FATAL message from Splunk should throw.")
        self.assertEquals(self.service_checks[0]['status'], 2, "service check should have status AgentCheck.CRITICAL")


class TestSplunkMetricIndividualDispatchFailures(AgentCheckTest):
    """
    Splunk metric check shouldn't fail if individual failures occur when dispatching Splunk searches
    """

    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "match": ".*metrics",
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

        data['saved_searches'] = ["minimal_metrics", "full_metrics"]

        def _mocked_dispatch_saved_search(*args, **kwargs):
            name = args[1].name
            if name == "full_metrics":
                raise Exception("BOOM")
            else:
                return name

        thrown = False

        try:
            self.run_check(config, mocks={
                '_auth_session': _mocked_auth_session,
                "_saved_searches": _mocked_saved_searches,
                "_dispatch_saved_search": _mocked_dispatch_saved_search,
                '_search': _mocked_search
            })
        except Exception:
            thrown = True

        self.assertFalse(thrown, "No exception should be thrown because minimal_metrics should succeed")
        self.assertEqual(len(self.metrics), 2)
        self.assertMetric(
            'metric_name',
            time=1488974400.0,
            value=1.0,
            tags=[])
        self.assertMetric(
            'metric_name',
            time=1488974400.0,
            value=2,
            tags=[])

        self.assertEqual(len(self.service_checks), 1)
        self.assertEquals(self.service_checks[0]['status'], 1, "service check should have status AgentCheck.WARNING")
        self.assertEquals(self.service_checks[0]['message'], "Failed to dispatch saved search 'full_metrics' due to: BOOM")

class TestSplunkMetricIndividualSearchFailures(AgentCheckTest):
    """
    Splunk metric check shouldn't fail if individual failures occur when executing Splunk searches
    """

    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "match": ".*metrics",
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

        data['saved_searches'] = ["minimal_metrics", "full_metrics"]

        def _mocked_failing_search(*args, **kwargs):
            sid = args[1].name
            if sid == "full_metrics":
                raise Exception("BOOM")
            else:
                return _mocked_search(*args, **kwargs)

        thrown = False

        try:
            self.run_check(config, mocks={
                '_auth_session': _mocked_auth_session,
                "_saved_searches": _mocked_saved_searches,
                "_dispatch_saved_search": _mocked_dispatch_saved_search,
                "_search": _mocked_failing_search
            })
        except Exception:
            thrown = True

        self.assertFalse(thrown, "No exception should be thrown because minimal_metrics should succeed")
        self.assertEqual(len(self.metrics), 2)
        self.assertMetric(
            'metric_name',
            time=1488974400.0,
            value=1.0,
            tags=[])
        self.assertMetric(
            'metric_name',
            time=1488974400.0,
            value=2,
            tags=[])

        self.assertEqual(len(self.service_checks), 1)
        self.assertEquals(self.service_checks[0]['status'], 1, "service check should have status AgentCheck.WARNING")
        self.assertEquals(self.service_checks[0]['message'],
                          "Failed to execute dispatched search 'full_metrics' with id full_metrics due to: BOOM")


class TestSplunkMetricSearchFullFailure(AgentCheckTest):
    """
    Splunk metric check should fail when all saved searches fail
    """

    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "full_metrics",
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

        data['saved_searches'] = ["full_metrics"]

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


class TestSplunkMetricRespectParallelDispatches(AgentCheckTest):
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        saved_searches_parallel = 2

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
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

            return True

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_and_await_search': _mock_dispatch_and_await_search,
            '_saved_searches': _mocked_saved_searches
        })

class TestSplunkSelectiveFieldsForIdentification(AgentCheckTest):
    """
    Splunk metrics check should process metrics where the unique identifier is set to a selective number of fields
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "metrics_identification_fields_selective",
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
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.metrics), 2)
        self.assertMetric(
            'metric_name',
            time=1923825600.0,
            value=1,
            tags=["uid1:uid", "uid2:1"])
        self.assertMetric(
            'metric_name',
            time=1923825600.0,
            value=2,
            tags=["uid1:uid", "uid2:2"])

        # shouldn't resend the metrics
        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_finalize_sid': _mocked_finalize_sid_none
        })

        self.assertEqual(len(self.metrics), 0)


class TestSplunkAllFieldsForIdentification(AgentCheckTest):
    """
    Splunk metrics check should process metrics where the unique identifier is set to all fields in a record
    """
    CHECK_NAME = 'splunk_metric'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "metrics_identification_fields_all",
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
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.metrics), 2)
        self.assertMetric(
            'metric_name',
            time=1923825600,
            value=1,
            tags=[])
        self.assertMetric(
            'metric_name',
            time=1923825600,
            value=2,
            tags=[])


        # shouldn't resend the metrics
        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches,
            '_finalize_sid': _mocked_finalize_sid_none
        })

        self.assertEqual(len(self.metrics), 0)


class TestSplunkDefaults(AgentCheckTest):
    CHECK_NAME = 'splunk_metric'

    def test_default_parameters(self):
        """
        when no default parameters are provided, the code should provide the parameters
        """
        config = {
            'init_config': {},
            'instances': [
                {
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "minimal_metrics"
                    }],
                    'tags': []
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
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.metrics), 2)


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
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "minimal_metrics"
                    }],
                    'tags': []
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
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.metrics), 2)


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
                    'url': 'http://localhost:13001',
                    'username': "admin",
                    'password': "admin",
                    'saved_searches': [{
                        "name": "minimal_metrics",
                        "parameters": {
                            "respect": "me"
                        }
                    }],
                    'tags': []
                }
            ]
        }

        expected_default_parameters = {'respect': 'me'}

        def _mocked_auth_session_to_check_instance_config(instance):
            for saved_search in instance.saved_searches.searches:
                self.assertEqual(saved_search.parameters, expected_default_parameters, msg="Unexpected overwritten default parameters for saved search: %s" % saved_search.name)
            return "sessionKey1"

        self.run_check(config, mocks={
            '_auth_session': _mocked_auth_session_to_check_instance_config,
            '_dispatch_saved_search': _mocked_dispatch_saved_search,
            '_search': _mocked_search,
            '_saved_searches': _mocked_saved_searches
        })

        self.assertEqual(len(self.metrics), 2)
