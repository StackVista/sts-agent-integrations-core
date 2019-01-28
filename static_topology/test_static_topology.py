# stdlib
import os

# 3p
from mock import Mock, MagicMock
from pyVmomi import vim  # pylint: disable=E0611
import simplejson as json
import mock

# datadog
from tests.checks.common import AgentCheckTest, Fixtures
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


class TestStaticCSVTopology(AgentCheckTest):
  """
  Unit tests for Static Topology AgentCheck.
  """
  CHECK_NAME = "static_topology"

  config = {
    'init_config': {},
    'instances': [
      {
        'type': 'csv',
        'components_file': '/dev/null',
        'relations_file': '/dev/null',
        'delimiter': ';'
      }
    ]
  }

  def test_omitted_component_file(self):
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
      self.assertTrue('Static topology instance missing "components_file" value.' in context.exception)

  def test_empty_component_file(self):
    config = {
      'init_config': {},
      'instances': [
        {
          'type': 'csv',
          'components_file': '/dev/null',
          'relations_file': '/dev/null',
          'delimiter': ';'
        }
      ]
    }
    with self.assertRaises(CheckException) as context:
      self.run_check(config)
    self.assertEquals('Component CSV file is empty.', str(context.exception))

  def test_empty_relation_file(self):
    config = {
      'init_config': {},
      'instances': [
        {
          'type': 'csv',
          'components_file': '/ignored/',
          'relations_file': '/dev/null',
          'delimiter': ';'
        }
      ]
    }

    with self.assertRaises(CheckException) as context:
      self.run_check(config, mocks={
        'handle_component_csv': lambda instance_key,filelocation,delimiter: None
      })

    self.assertEquals('Relation CSV file is empty.', str(context.exception))

  @mock.patch('codecs.open',
              side_effect=lambda location,mode,encoding: MockFileReader(location, {
                'component.csv': ['id,name,type', '1,name1,type1', '2,name2,type2'],
                'relation.csv': ['sourceid,targetid,type', '1,2,type']}))
  def test_topology(self, mock):
    config = {
      'init_config': {},
      'instances': [
        {
          'type': 'csv',
          'components_file': 'component.csv',
          'relations_file': 'relation.csv',
          'delimiter': ','
        }
      ]
    }
    self.run_check(config)
    instances = self.check.get_topology_instances()
    self.assertEqual(len(instances), 1)
    self.assertEqual(len(instances[0]['components']), 2)
    self.assertEqual(len(instances[0]['relations']), 1)

  @mock.patch('codecs.open', side_effect = lambda location,mode,encoding: MockFileReader(location, {
                'component.csv': ['NOID,name,type'],
                'relation.csv': []}))
  def test_missing_component_id_field(self, mock):
    config = {
      'init_config': {},
      'instances': [
        {
          'type': 'csv',
          'components_file': 'component.csv',
          'delimiter': ','
        }
      ]
    }

    with self.assertRaises(CheckException) as context:
      self.run_check(config)

    self.assertEquals('CSV header id not found in component csv.', str(context.exception))

  @mock.patch('codecs.open', side_effect=lambda location, mode, encoding: MockFileReader(location, {
    'component.csv': ['id,NONAME,type'],
    'relation.csv': []}))
  def test_missing_component_name_field(self, mock):
    config = {
      'init_config': {},
      'instances': [
        {
          'type': 'csv',
          'components_file': 'component.csv',
          'delimiter': ','
        }
      ]
    }

    with self.assertRaises(CheckException) as context:
      self.run_check(config)

    self.assertEquals('CSV header name not found in component csv.', str(context.exception))

  @mock.patch('codecs.open', side_effect=lambda location, mode, encoding: MockFileReader(location, {
    'component.csv': ['id,name,NOTYPE'],
    'relation.csv': []}))
  def test_missing_component_id_field(self, mock):
    config = {
      'init_config': {},
      'instances': [
        {
          'type': 'csv',
          'components_file': 'component.csv',
          'delimiter': ','
        }
      ]
    }

    with self.assertRaises(CheckException) as context:
      self.run_check(config)

    self.assertEquals('CSV header type not found in component csv.', str(context.exception))

  @mock.patch('codecs.open', side_effect=lambda location, mode, encoding: MockFileReader(location, {
    'component.csv': ['id,name,type'],
    'relation.csv': ['NOSOURCEID,targetid,type']}))
  def test_missing_relation_sourceid_field(self, mock):
    config = {
      'init_config': {},
      'instances': [
        {
          'type': 'csv',
          'components_file': 'component.csv',
          'relations_file': 'relation.csv',
          'delimiter': ','
        }
      ]
    }

    with self.assertRaises(CheckException) as context:
      self.run_check(config)

    self.assertEquals('CSV header sourceid not found in relation csv.', str(context.exception))

    @mock.patch('codecs.open', side_effect=lambda location, mode, encoding: MockFileReader(location, {
      'component.csv': ['id,name,type'],
      'relation.csv': ['sourceid,NOTARGETID,type']}))
    def test_missing_relation_targetid_field(self, mock):
      config = {
        'init_config': {},
        'instances': [
          {
            'type': 'csv',
            'components_file': 'component.csv',
            'relations_file': 'relation.csv',
            'delimiter': ','
          }
        ]
      }

      with self.assertRaises(CheckException) as context:
        self.run_check(config)

      self.assertEquals('CSV header targetid not found in relation csv.', str(context.exception))

    @mock.patch('codecs.open', side_effect=lambda location, mode, encoding: MockFileReader(location, {
      'component.csv': ['id,name,type'],
      'relation.csv': ['sourceid,targetid,NOTYPE']}))
    def test_missing_relation_type_field(self, mock):
      config = {
        'init_config': {},
        'instances': [
          {
            'type': 'csv',
            'components_file': 'component.csv',
            'relations_file': 'relation.csv',
            'delimiter': ','
          }
        ]
      }

      with self.assertRaises(CheckException) as context:
        self.run_check(config)

      self.assertEquals('CSV header type not found in relation csv.', str(context.exception))
