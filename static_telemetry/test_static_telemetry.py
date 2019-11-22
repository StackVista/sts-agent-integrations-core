# stdlib
import os

# 3p
import mock

from tests.checks.common import AgentCheckTest
from checks import CheckException

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), 'ci')

class MockFileReader:
    """used to mock codec.open, it returns content for a with-construct."""

    def __init__(self, location, content):
        """
        :param location: the csv file location, used to return value of content[location]
        :param content: dict(key: location, value: array of to delimit strings)
        """
        self.content = content
        self.location = location

    def __enter__(self):
        return self.content[self.location]

    def __exit__(self, type, value, traceback):
        pass


class TestStaticCSVTelemetry(AgentCheckTest):
    """
    Unit tests for Static Telemetry AgentCheck.
    """
    CHECK_NAME = "static_telemetry"

    config = {
        'init_config': {},
        'instances': [
            {
                'type': 'csv',
                'events_file': '/dev/null',
                'delimiter': ';'
            }
        ]
    }

    def test_omitted_type(self):
        config = {
            'init_config': {},
            'instances': [
                {
                    'events_file': 'events.csv',
                    'delimiter': ';'
                }
            ]
        }
        with self.assertRaises(CheckException) as context:
            self.run_check(config)
            self.assertTrue('Static telemetry instance missing "type" value.' in context.exception)

    def test_omitted_events_file(self):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'delimiter': ';'
                }
            ]
        }
        with self.assertRaises(CheckException) as context:
            self.run_check(config)
            self.assertTrue('Static telemetry instance missing "events_file" value.' in context.exception)

    def test_omitted_delimiter(self):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'events_file': 'events.csv'
                }
            ]
        }
        with self.assertRaises(CheckException) as context:
            self.run_check(config)
            self.assertTrue('Static telemetry instance missing "delimiter" value.' in context.exception)

    def test_empty_events_file(self):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'events_file': '/dev/null',
                    'delimiter': ';'
                }
            ]
        }
        with self.assertRaises(CheckException) as context:
            self.run_check(config)
        self.assertEquals('Events CSV file is empty.', str(context.exception))

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
                    'events.csv': ['host,header1,header2', 'host1,value1,value2']}))
    def test_events(self, mock):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'events_file': 'events.csv',
                    'delimiter': ','
                }
            ]
        }
        self.run_check(config)
        self.assertEqual(len(self.events), 1)
        event = self.events[0]
        self.assertEqual(event['host'], 'host1')
        self.assertTrue('header1:value1' in event['tags'], 'header1:value1 tag not found.')
        self.assertTrue('header2:value2' in event['tags'], 'header2:value2 tag not found.')

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
                    'events.csv': ['host,header1,epoch,header2', 'host1,value1,1573654995,value2']}))
    def test_events_with_epoch_field(self, mock):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'events_file': 'events.csv',
                    'delimiter': ',',
                    'timestamp_field': 'epoch'
                }
            ]
        }
        self.run_check(config)
        self.assertEqual(len(self.events), 1)
        event = self.events[0]
        self.assertEqual(event['host'], 'host1')
        self.assertTrue('header1:value1' in event['tags'], 'header1:value1 tag not found.')
        self.assertTrue('header2:value2' in event['tags'], 'header2:value2 tag not found.')
        self.assertTrue(event['timestamp'] == 1573654995, "Timestamp does not match")

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
                    'events.csv': ['host,header1,header2', 'host1,value1,value2']}))
    def test_events_epoch_field_set_but_omitted(self, mock):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'events_file': 'events.csv',
                    'delimiter': ',',
                    'timestamp_field': 'epoch'
                }
            ]
        }
        with self.assertRaises(CheckException) as context:
            self.run_check(config)
            self.assertTrue('CSV header "epoch" not found in csv.' in context.exception)

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
                    'events.csv': ['host,header1,epoch,header2', 'host1,value1,1573654995,value2', 'host2,value3,,value4']}))
    def test_events_epoch_field_set_but_no_value(self, mock):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'events_file': 'events.csv',
                    'delimiter': ',',
                    'timestamp_field': 'epoch'
                }
            ]
        }
        self.run_check(config)
        self.assertEqual(len(self.events), 1)  # should have skipped the second event
        event = self.events[0]
        self.assertEqual(event['host'], 'host1')
        self.assertTrue('header1:value1' in event['tags'], 'header1:value1 tag not found.')
        self.assertTrue('header2:value2' in event['tags'], 'header2:value2 tag not found.')
        self.assertTrue(event['timestamp'] == 1573654995, "Timestamp does not match")

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
                    'events.csv': ['host,header1,epoch,header2', 'host1,value1,incorrectepochvalue,value2',
                                   'host2,value3,,value4']}))
    def test_events_epoch_field_unparsable(self, mock):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'events_file': 'events.csv',
                    'delimiter': ',',
                    'timestamp_field': 'epoch'
                }
            ]
        }
        self.run_check(config)
        self.assertEqual(len(self.events), 0)  # should have skipped the event because of parsing issues
