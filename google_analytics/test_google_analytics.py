# stdlib
import os

# 3p
import json
from mock import Mock

# datadog
from tests.checks.common import AgentCheckTest, Fixtures

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), 'ci')


class GoogleAnalytics(AgentCheckTest):
    CHECK_NAME = "google_analytics"

    @staticmethod
    def _config(instances):
        def include_profile(instance):
            instance['profile'] = 'ga:12345678'
            return instance

        return {
            'init_config': {
                'key_file_location': '/dev/null'
            },
            'instances': map(lambda instance: include_profile(instance), instances)
        }

    @staticmethod
    def _get_json(file_name):
        return json.loads(Fixtures.read_file("%s.json" % file_name, sdk_dir=FIXTURE_DIR))


class TestRealtimeGoogleAnalytics(GoogleAnalytics):
    """
    Unit tests for Real time Google Analytics AgentCheck.
    """

    def test_detect_real_time_required(self):
        process_realtime_mock = Mock()
        process_ga_mock = Mock()

        self.run_check(self._config([{
            'metrics': ['rt:pageviews']
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'process_realtime': process_realtime_mock,
            'process_ga': process_ga_mock
        })

        self.assertTrue(process_realtime_mock.called, msg='Method process_realtime should not have been called.')
        self.assertFalse(process_ga_mock.called, msg='Method process_ga should not have been called.')

    def test_detect_non_real_time_required(self):
        process_realtime_mock = Mock()
        process_ga_mock = Mock()

        self.run_check(self._config([{
            'metrics': ['ga:pageviews']
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'process_realtime': process_realtime_mock,
            'process_ga': process_ga_mock
        })

        self.assertFalse(process_realtime_mock.called, msg='Method process_realtime should not have been called.')
        self.assertTrue(process_ga_mock.called, msg='Method process_ga should not have been called.')

    def test_metric(self):
        self.run_check(self._config([{
            'metrics': ['rt:pageviews'],
            'dimensions': ['rt:minutesAgo']
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'get_rt_results': lambda profile_id, metric, dimensions, filters: self._get_json("realtime_one_metric_one_dimension_no_filter"),
            'get_ga_results': lambda profile_id, metrics, dimensions, filters, start_time, end_time: None
        })
        self.assertMetric(metric_name="googleanalytics.rt.pageviews", value=40, tags=['profile:ga:12345678', 'rt.minutesAgo:01'], count=1)

    def test_one_metric_one_dimension_minutesAgo_no_filter_no_minute(self):
        self.run_check(self._config([{
            'is_realtime': True,
            'metrics': ['rt:pageviews'],
            'dimensions': ['rt:minutesAgo']
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'get_rt_results': lambda profile_id, metric, dimensions, filters: self._get_json("realtime_one_metric_one_dimension_no_filter_no_minute_value"),
            'get_ga_results': lambda profile_id, metrics, dimensions, filters, start_time, end_time: None
        })
        self.assertMetric(metric_name="googleanalytics.rt.pageviews", value=0, tags=['profile:ga:12345678', 'rt.minutesAgo:01'], count=1)

    def test_one_metric_empty_results(self):
        self.run_check(self._config([{
            'is_realtime': True,
            'metrics': ['rt:pageviews'],
            'dimensions': ['rt:minutesAgo']
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'get_rt_results': lambda profile_id, metric, dimensions, filters: self._get_json("realtime_one_metric_one_dimension_no_filter_no_result"),
            'get_ga_results': lambda profile_id, metrics, dimensions, filters, start_time, end_time: None
        })
        self.assertMetric(metric_name="googleanalytics.rt.pageviews", value=0,tags=['profile:ga:12345678', 'rt.minutesAgo:01'], count=1)

    def test_one_metric_no_dimension_no_filter(self):
        self.run_check(self._config([{
            'is_realtime': True,
            'metrics': ['rt:activeUsers'],
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'get_rt_results': lambda profile_id, metric, dimensions, filters: self._get_json("realtime_one_metric_no_dimensions_no_filter"),
            'get_ga_results': lambda profile_id, metrics, dimensions, filters, start_time, end_time: None
        })
        self.assertMetric(metric_name="googleanalytics.rt.activeUsers", value=2, tags=['profile:ga:12345678'], count=1)
        self.assertEqual(len(self.metrics), 1, msg='One metric should have been collected.')

    def test_one_metric_two_dimensions_one_filter(self):
        self.run_check(self._config([{
            'is_realtime': True,
            'metrics': ['rt:pageviews'],
            'dimensions': ['rt:minutesAgo', 'rt:pagePath'],
            'filters': 'rt:pagePath=~^/booker/selection.outbound'
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'get_rt_results': lambda profile_id, metric, dimensions, filters: self._get_json("realtime_one_metric_two_dimensions_one_filter"),
            'get_ga_results': lambda profile_id, metrics, dimensions, filters, start_time, end_time: None
        })
        self.assertMetric(metric_name="googleanalytics.rt.pageviews", value=1, tags=['profile:ga:12345678', 'rt.minutesAgo:01', 'rt.pagePath:/booker/selection.outbound'], count=1)
        self.assertMetric(metric_name="googleanalytics.rt.pageviews", value=2, tags=['profile:ga:12345678', 'rt.minutesAgo:01', 'rt.pagePath:/booker/selection.outbound.connections'], count=1)
        self.assertMetric(metric_name="googleanalytics.rt.pageviews", value=2, tags=['profile:ga:12345678', 'rt.minutesAgo:01', 'rt.pagePath:/booker/selection.outbound.connections/bookingSummaryModal'], count=1)
        self.assertMetric(metric_name="googleanalytics.rt.pageviews", value=1, tags=['profile:ga:12345678', 'rt.minutesAgo:01', 'rt.pagePath:/booker/selection.outbound.detail'], count=1)
        self.assertEqual(len(self.metrics), 4, msg='Four metrics should have been collected.')

    def test_metric_instance_tags(self):
        self.run_check(self._config([{
            'is_realtime': True,
            'metrics': ['rt:pageviews'],
            'dimensions': ['rt:minutesAgo'],
            'tags': ['env:test', 'key:value']
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'get_rt_results': lambda profile_id, metric, dimensions, filters: self._get_json("realtime_one_metric_one_dimension_no_filter"),
            'get_ga_results': lambda profile_id, metrics, dimensions, filters, start_time, end_time: None
        })
        self.assertMetric(metric_name="googleanalytics.rt.pageviews", value=40, tags=['profile:ga:12345678', 'rt.minutesAgo:01', 'env:test', 'key:value'], count=1)


class TestGoogleAnalytics(GoogleAnalytics):
    """
    Unit tests for non- real time Google Analytics AgentCheck.
    """

    def test_empty_metrics(self):
        self.run_check(self._config([{
            'is_realtime': False,
            'metrics': ['ga:pageviews', 'ga:users'],
            'dimensions': ['ga:pagePath', 'ga:browser'],
            'filters': ['ga:pagePath==/booker_v3/confirmation', 'ga:browser==Chrome2'],
            'start_time': '2daysAgo',
            'end_time': '1daysAgo'
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'get_rt_results': lambda profile_id, metric, dimensions, filters: None,
            'get_ga_results': lambda profile_id, metrics, dimensions, filters, start_time, end_time: self._get_json("ga_two_metrics_two_dimensions_two_filters_no_results")
        })
        self.assertEqual(len(self.metrics), 0, msg='No metrics should have been collected.')

    def test_empty_dimensions(self):
        self.run_check(self._config([{
            'is_realtime': False,
            'metrics': ['ga:pageviews'],
            'start_time': '2daysAgo',
            'end_time': '1daysAgo'
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'get_rt_results': lambda profile_id, metric, dimensions, filters: None,
            'get_ga_results': lambda profile_id, metrics, dimensions, filters, start_time, end_time: self._get_json("ga_one_metric_no_dimensions_no_filters")
        })
        self.assertMetric(metric_name="googleanalytics.ga.pageviews", value=8119, tags=['profile:ga:12345678'], count=1)
        self.assertEqual(len(self.metrics), 1, msg='One metric should have been collected.')

    def test_one_metric_one_dimension_one_filter(self):
        self.run_check(self._config([{
            'is_realtime': False,
            'metrics': ['ga:pageviews'],
            'dimensions': ['ga:pagePath'],
            'filters': 'ga:pagePath==/booker_v3/confirmation',
            'start_time': '2daysAgo',
            'end_time': '1daysAgo'
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'get_rt_results': lambda profile_id, metric, dimensions, filters: None,
            'get_ga_results': lambda profile_id, metrics, dimensions, filters, start_time, end_time: self._get_json("ga_one_metric_one_dimension_one_filter")
        })
        self.assertMetric(metric_name="googleanalytics.ga.pageviews", value=11, tags=['profile:ga:12345678','ga.pagePath:/booker/confirmation'], count=1)
        self.assertEqual(len(self.metrics), 1, msg='One metric should have been collected.')

    def test_one_metric_one_dimension_no_filter(self):
        self.run_check(self._config([{
            'is_realtime': False,
            'metrics': ['ga:pageviews'],
            'dimensions': ['ga:pagePath'],
            'start_time': '2daysAgo',
            'end_time': '1daysAgo'
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'get_rt_results': lambda profile_id, metric, dimensions, filters: None,
            'get_ga_results': lambda profile_id, metrics, dimensions, filters, start_time, end_time: self._get_json("ga_one_metric_one_dimension_no_filter")
        })
        self.assertMetric(metric_name="googleanalytics.ga.pageviews", value=29, tags=['profile:ga:12345678', 'ga.pagePath:/'], count=1)
        self.assertMetric(metric_name="googleanalytics.ga.pageviews", value=39, tags=['profile:ga:12345678', 'ga.pagePath:/aftersales/cancel-step-1'], count=1)
        self.assertMetric(metric_name="googleanalytics.ga.pageviews", value=15, tags=['profile:ga:12345678', 'ga.pagePath:/aftersales/cancel-step-2'], count=1)
        self.assertMetric(metric_name="googleanalytics.ga.pageviews", value=0, tags=['profile:ga:12345678', 'ga.pagePath:/nl/tickets'], count=1)
        self.assertMetric(metric_name="googleanalytics.ga.pageviews", value=0, tags=['profile:ga:12345678', 'ga.pagePath:/nl/tickets-v3/'], count=1)
        self.assertEqual(len(self.metrics), 5, msg='Five metrics should have been collected.')

    def test_two_metrics_one_dimension_one_filter(self):
        self.run_check(self._config([{
            'is_realtime': False,
            'metrics': ['ga:pageviews', 'ga:users'],
            'dimensions': ['ga:pagePath'],
            'filters': ['ga:pagePath==/booker_v3/confirmation'],
            'start_time': '2daysAgo',
            'end_time': '1daysAgo'
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'get_rt_results': lambda profile_id, metric, dimensions, filters: None,
            'get_ga_results': lambda profile_id, metrics, dimensions, filters, start_time, end_time: self._get_json("ga_two_metrics_one_dimension_one_filter")
        })
        self.assertMetric(metric_name="googleanalytics.ga.pageviews", value=11, tags=['profile:ga:12345678', 'ga.pagePath:/booker/confirmation'], count=1)
        self.assertMetric(metric_name="googleanalytics.ga.users", value=6, tags=['profile:ga:12345678', 'ga.pagePath:/booker/confirmation'], count=1)
        self.assertEqual(len(self.metrics), 2, msg='Two metrics should have been collected.')

    def test_two_metrics_two_dimensions_two_filters(self):
        self.run_check(self._config([{
            'is_realtime': False,
            'metrics': ['ga:pageviews', 'ga:users'],
            'dimensions': ['ga:pagePath', 'ga:browser'],
            'filters': ['ga:pagePath==/booker_v3/confirmation', 'ga:browser==Chrome'],
            'start_time': '2daysAgo',
            'end_time': '1daysAgo'
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'get_rt_results': lambda profile_id, metric, dimensions, filters: None,
            'get_ga_results': lambda profile_id, metrics, dimensions, filters, start_time, end_time: self._get_json("ga_two_metrics_two_dimensions_two_filters")
        })
        self.assertMetric(metric_name="googleanalytics.ga.pageviews", value=11, tags=['profile:ga:12345678', 'ga.pagePath:/booker/confirmation', 'ga.browser:Chrome'], count=1)
        self.assertMetric(metric_name="googleanalytics.ga.users", value=6, tags=['profile:ga:12345678', 'ga.pagePath:/booker/confirmation', 'ga.browser:Chrome'], count=1)
        self.assertEqual(len(self.metrics), 2, msg='Two metrics should have been collected.')

    def test_instance_tags(self):
        self.run_check(self._config([{
            'is_realtime': False,
            'metrics': ['ga:pageviews'],
            'start_time': '2daysAgo',
            'end_time': '1daysAgo',
            'tags': ['tag:tag1', 'key:value']
        }]), mocks={
            'get_ga_service': lambda api_name, api_version, scope, key_file_location: None,
            'get_rt_results': lambda profile_id, metric, dimensions, filters: None,
            'get_ga_results': lambda profile_id, metrics, dimensions, filters, start_time, end_time: self._get_json("ga_one_metric_no_dimensions_no_filters")
        })
        self.assertMetric(metric_name="googleanalytics.ga.pageviews", value=8119, tags=['profile:ga:12345678','tag:tag1', 'key:value'], count=1)
        self.assertEqual(len(self.metrics), 1, msg='One metric should have been collected.')
