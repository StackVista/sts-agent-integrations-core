"""
Google Analytics check
Collects metrics from the Analytics API.
"""

# the following try/except block will make the custom check compatible with any Agent version
try:
    # first, try to import the base class from old versions of the Agent...
    from checks import AgentCheck
except ImportError:
    # ...if the above failed, the check is running in Agent version 6 or later
    from datadog_checks.checks import AgentCheck

from google.oauth2 import service_account
import googleapiclient.discovery


class GoogleAnalyticsCheck(AgentCheck):
    """
    Collects metrics based on what is defined in the yaml
    """

    scope = ['https://www.googleapis.com/auth/analytics.readonly']
    service = None
    apiName = 'analytics'
    version = 'v3'

    def check(self, instance):
        if not self.service:
            self.service = self.get_ga_service(
                self.apiName,
                self.version,
                self.scope,
                self.init_config.get('key_file_location'))

        profile = instance.get('profile')
        instance_tags = instance.get('tags', [])
        instance_tags.append("profile:" + profile)
        metrics = instance.get('metrics', [])
        dimensions = instance.get('dimensions', [])
        filters = instance.get('filters', [])

        is_realtime = reduce((lambda is_rt, metric: is_rt or metric[:3] == "rt:"), metrics, False)
        self.log.debug("Use of real time API: %s" % is_realtime)

        if is_realtime:
            self.log.debug('profile: %s, real-time, metric: %s, dimensions: %s' % (profile, metrics, dimensions))
            self.process_realtime(profile, instance_tags, metrics, dimensions, filters)
        else:  # non- real time queries
            start_time = instance.get('start_time')
            end_time = instance.get('end_time')
            self.log.debug('profile: %s, metrics: %s, dimensions: %s, start_time: %s, end_time:%s' % (profile, metrics, dimensions, start_time, end_time))
            self.process_ga(profile, instance_tags, metrics, dimensions, filters, start_time, end_time)

    def process_realtime(self, profile, instance_tags, metrics, dimensions, filters):
        """
        Process real time Google Analytics API request
        :param profile: Google Analytics Profile ID
        :param instance_tags: tags to add to the retrieved metrics as set in the yaml
        :param metrics: the selected Google Analytics metrics as specified in the yaml
        :param dimensions: the selected Google Analytics dimensions as specified in the yaml
        :param filters: the selected Google Analytics filters as specified in the yaml
        :return: nothing
        """
        result = self.get_rt_results(profile, metrics, dimensions, filters)
        self.log.debug("Real time response: %s" % result)

        if "rt:minutesAgo" in dimensions:
            header_names = [header.get('name') for header in result.get('columnHeaders')]
            minutes_ago_idx = header_names.index("rt:minutesAgo")

            # In order to have a consistent metric, we look for the value of one minute ago and not during the last minute.
            rows = result.get('rows', [])
            filtered_rows = filter(lambda row: int(row[minutes_ago_idx]) == 1, rows)
            result['rows'] = filtered_rows

        self.process_response(instance_tags, result)

    def process_ga(self, profile, instance_tags, metrics, dimensions, filters, start_time, end_time):
        """
        Process non- real time Google Analytics API request
        :param profile: Google Analytics Profile ID
        :param instance_tags: tags to add to the retrieved metrics as set in the yaml
        :param metrics: the selected Google Analytics metrics as specified in the yaml
        :param dimensions: the selected Google Analytics dimensions as specified in the yaml
        :param filters: the selected Google Analytics filters as specified in the yaml
        :param start_time: the selected Google Analytics start time of the query as specified in the yaml
        :param end_time: the selected Google Analytics end time of the query as specified in the yaml
        :return: nothing
        """

        result = self.get_ga_results(profile, metrics, dimensions, filters, start_time, end_time)
        self.log.debug("GA response: %s" % result)
        self.process_response(instance_tags, result)

    def process_response(self, instance_tags, result):
        """
        Process Google Analytics (real time) API responses and emits metrics.
        :param instance_tags: tags to add to the retrieved metrics as set in the yaml
        :param result: the API response in json
        :return: nothing
        """
        def replace(string):
            """
            replaces the following prefixes for metric key and tag compatibility
            - 'ga:' to 'ga.'
            - 'rt:' to 'rt.'
            :param string: input
            :return: transformed input
            """
            if string[:3] == "ga:" or string[:3] == "rt:":
                return string.replace(':', '.', 1)
            return string

        # extract the location of the dimensions and metrics
        headers = result.get('columnHeaders')
        dimensions = []
        dimension_indices = []
        metrics = []
        metric_indices = []
        for header_idx in xrange(len(headers)):
            header = headers[header_idx]
            header_name = header.get('name')
            header_column_type = header.get("columnType")
            if header_column_type == "DIMENSION":
                dimensions += [replace(header_name)]
                dimension_indices += [header_idx]
            elif header_column_type == "METRIC":
                metrics += [replace(header_name)]
                metric_indices += [header_idx]

        # iterate over the resulting results, rows are not always present
        rows = result.get('rows', [])
        for row in rows:
            extracted_dimensions = [row[dimension_idx] for dimension_idx in dimension_indices]
            extracted_metrics = [row[dimension_idx] for dimension_idx in metric_indices]
            dimensions_with_keys = zip(dimensions, extracted_dimensions)
            metrics_with_keys = zip(metrics, extracted_metrics)

            tags = []
            tags += instance_tags
            tags += map(lambda (k, v): "%s:%s" % (k, v), dimensions_with_keys)

            for metric_key, metric_value in metrics_with_keys:
                self.gauge("googleanalytics.%s" % metric_key, int(metric_value), tags=tags)

    def get_ga_service(self, api_name, api_version, scope, key_file_location):
        """
        Get Google Analytics API object
        :param api_name: Google Analytics API name to use
        :param api_version: Google Analytics API version to use
        :param scope: Google Analytics API scope to use
        :param key_file_location: the file location of the Google credentials JSON
        :return: Google Resource for interacting with the service.
        """
        credentials = service_account.Credentials.from_service_account_file(key_file_location, scopes=scope)
        service = googleapiclient.discovery.build(api_name, api_version, credentials=credentials)
        return service

    def get_rt_results(self, profile_id, metrics, dimensions=None, filters=None):
        """
        Obtain real time results from Google Analytics API
        :param profile_id: Google Analytics Profile ID
        :param metrics: list, the selected Google Analytics metrics as specified in the yaml
        :param dimensions: list, the selected Google Analytics dimensions as specified in the yaml
        :param filters: the selected Google Analytics filters as specified in the yaml
        :return: json
        """
        filters = ';'.join(filters) if filters else None  # Default filter is AND, comma is OR
        dimensions = ','.join(dimensions) if dimensions else None
        return self.service.data().realtime().get(
            ids=profile_id,
            metrics=','.join(metrics),
            dimensions=dimensions,
            filters=filters
        ).execute()

    def get_ga_results(self, profile_id, metrics, dimensions, filters, start_time, end_time):
        """
        Obtain non- real time results from Google Analytics API
        :param profile_id: Google Analytics Profile ID
        :param metrics: list, the selected Google Analytics metrics as specified in the yaml
        :param dimensions: list, the selected Google Analytics dimensions as specified in the yaml
        :param filters: the selected Google Analytics filters as specified in the yaml
        :param start_time: the selected Google Analytics start time of the query as specified in the yaml
        :param end_time: the selected Google Analytics end time of the query as specified in the yaml
        :return: json
        """
        filters = ';'.join(filters) if filters else None # Default filter is AND, comma is OR
        dimensions = ','.join(dimensions) if dimensions else None
        return self.service.data().ga().get(
            ids=profile_id,
            start_date=start_time,
            end_date=end_time,
            metrics=','.join(metrics),
            dimensions=dimensions,
            filters=filters
        ).execute()
