# stdlib
import os

# 3p
import mock

# datadog
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

    def test_omitted_relation_file(self):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'components_file': '/dev/null',
                    'delimiter': ';'
                }
            ]
        }
        with self.assertRaises(CheckException) as context:
            self.run_check(config)
        self.assertTrue('Static topology instance missing "relations_file" value.' in context.exception)

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
                'handle_component_csv': lambda instance_key, filelocation, delimiter, instance_tags: None
            })

        self.assertEquals('Relation CSV file is empty.', str(context.exception))

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
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

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
                    'component.csv': ['id,name,type', '1,name1,type1', '2,name2,type2'],
                    'relation.csv': ['sourceid,targetid,type', '1,2,type']}))
    def test_snapshot(self, mock):
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
        self.assertTrue(instances[0]['start_snapshot'], msg='start_snapshot was not set to True')
        self.assertTrue(instances[0]['stop_snapshot'], msg='stop_snapshot was not set to True')

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
                    'component.csv': ['id,name,type', '1,name1,type1', '2,name2,type2'],
                    'relation.csv': ['sourceid,targetid,type', '1,2,type']}))
    def test_topology_with_instance_tags(self, mock):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'components_file': 'component.csv',
                    'relations_file': 'relation.csv',
                    'delimiter': ',',
                    'tags': ['tag1', 'tag2']
                }
            ]
        }
        self.run_check(config)
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(len(instances[0]['components']), 2)
        self.assertEqual(len(instances[0]['components'][0]['data']['labels']), 4)
        self.assertEqual(len(instances[0]['components'][1]['data']['labels']), 4)
        self.assertIn("csv.component:component.csv", instances[0]['components'][0]['data']['labels'])
        self.assertIn("csv.relation:relation.csv", instances[0]['components'][0]['data']['labels'])
        self.assertIn("csv.component:component.csv", instances[0]['components'][1]['data']['labels'])
        self.assertIn("csv.relation:relation.csv", instances[0]['components'][1]['data']['labels'])

        self.assertEqual(len(instances[0]['relations']), 1)
        self.assertNotIn('labels', instances[0]['relations'][0]['data'])

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
                    'component.csv': ['id,name,type,labels', '1,name1,type1,label1', '2,name2,type2,'],
                    'relation.csv': ['sourceid,targetid,type', '1,2,type']}))
    def test_topology_with_labels(self, mock):
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
        self.assertEqual(len(instances[0]['components'][0]['data']['labels']), 3)
        self.assertEqual(len(instances[0]['components'][1]['data']['labels']), 2)
        self.assertIn("csv.component:component.csv", instances[0]['components'][0]['data']['labels'])
        self.assertIn("csv.relation:relation.csv", instances[0]['components'][0]['data']['labels'])
        self.assertIn("csv.component:component.csv", instances[0]['components'][1]['data']['labels'])
        self.assertIn("csv.relation:relation.csv", instances[0]['components'][1]['data']['labels'])

        self.assertEqual(len(instances[0]['relations']), 1)
        self.assertNotIn('labels', instances[0]['relations'][0]['data'])

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
                    'component.csv': ['id,name,type,labels', '1,name1,type1,"label1,label2"', '2,name2,type2,"label1,label2,label3"'],
                    'relation.csv': ['sourceid,targetid,type', '1,2,type']}))
    def test_topology_with_multiple_labels(self, mock):
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
        self.assertEqual(len(instances[0]['components'][0]['data']['labels']), 4)
        self.assertEqual(len(instances[0]['components'][1]['data']['labels']), 5)
        self.assertIn("label1", instances[0]['components'][0]['data']['labels'])
        self.assertIn("label2", instances[0]['components'][0]['data']['labels'])
        self.assertIn("csv.component:component.csv", instances[0]['components'][0]['data']['labels'])
        self.assertIn("csv.relation:relation.csv", instances[0]['components'][0]['data']['labels'])
        self.assertIn("label1", instances[0]['components'][1]['data']['labels'])
        self.assertIn("label2", instances[0]['components'][1]['data']['labels'])
        self.assertIn("label3", instances[0]['components'][1]['data']['labels'])
        self.assertIn("csv.component:component.csv", instances[0]['components'][1]['data']['labels'])
        self.assertIn("csv.relation:relation.csv", instances[0]['components'][1]['data']['labels'])

        self.assertEqual(len(instances[0]['relations']), 1)
        self.assertNotIn('labels', instances[0]['relations'][0]['data'])

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
                    'component.csv': ['id,name,type,identifiers', '1,name1,type1,id1','2,name2,type2,'],
                    'relation.csv': ['sourceid,targetid,type', '1,2,type']}))
    def test_topology_with_identifier(self, mock):
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
        self.assertEqual(len(instances[0]['components'][0]['data']['identifiers']), 1)
        self.assertEqual(len(instances[0]['components'][1]['data']['identifiers']), 0)
        self.assertIn("id1", instances[0]['components'][0]['data']['identifiers'])

        self.assertEqual(len(instances[0]['relations']), 1)
        self.assertNotIn('labels', instances[0]['relations'][0]['data'])

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
                    'component.csv': ['id,name,type,identifiers', '1,name1,type1,"id1,id2"','2,name2,type2,"id1,id2,id3"'],
                    'relation.csv': ['sourceid,targetid,type', '1,2,type']}))
    def test_topology_with_multiple_identifiers(self, mock):
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
        self.assertEqual(len(instances[0]['components'][0]['data']['identifiers']), 2)
        self.assertEqual(len(instances[0]['components'][1]['data']['identifiers']), 3)
        self.assertIn("id1", instances[0]['components'][0]['data']['identifiers'])
        self.assertIn("id2", instances[0]['components'][0]['data']['identifiers'])
        self.assertIn("id1", instances[0]['components'][1]['data']['identifiers'])
        self.assertIn("id2", instances[0]['components'][1]['data']['identifiers'])
        self.assertIn("id3", instances[0]['components'][1]['data']['identifiers'])

        self.assertEqual(len(instances[0]['relations']), 1)
        self.assertNotIn('labels', instances[0]['relations'][0]['data'])

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
                    'component.csv': ['id,name,type,labels', '1,name1,type1,label1', '2,name2,type2,'],
                    'relation.csv': ['sourceid,targetid,type', '1,2,type']}))
    def test_topology_with_labels_and_instance_tags(self, mock):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'components_file': 'component.csv',
                    'relations_file': 'relation.csv',
                    'delimiter': ',',
                    'tags': ['tag1', 'tag2']
                }
            ]
        }
        self.run_check(config)
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(len(instances[0]['components']), 2)
        self.assertEqual(len(instances[0]['components'][0]['data']['labels']), 5)
        self.assertEqual(len(instances[0]['components'][1]['data']['labels']), 4)
        self.assertIn("csv.component:component.csv", instances[0]['components'][0]['data']['labels'])
        self.assertIn("csv.relation:relation.csv", instances[0]['components'][0]['data']['labels'])
        self.assertIn("csv.component:component.csv", instances[0]['components'][1]['data']['labels'])
        self.assertIn("csv.relation:relation.csv", instances[0]['components'][1]['data']['labels'])

        self.assertEqual(len(instances[0]['relations']), 1)
        self.assertNotIn('labels', instances[0]['relations'][0]['data'])

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
                    'component.csv': ['id,name,type,environments', '1,name1,type1,env1', '2,name2,type2,'],
                    'relation.csv': ['sourceid,targetid,type', '1,2,type']}))
    def test_topology_with_environments(self, mock):
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
        self.assertEqual(len(instances[0]['components'][0]['data']['labels']), 2)
        self.assertEqual(len(instances[0]['components'][1]['data']['labels']), 2)
        self.assertIn("csv.component:component.csv", instances[0]['components'][0]['data']['labels'])
        self.assertIn("csv.relation:relation.csv", instances[0]['components'][0]['data']['labels'])
        self.assertIn("csv.component:component.csv", instances[0]['components'][1]['data']['labels'])
        self.assertIn("csv.relation:relation.csv", instances[0]['components'][1]['data']['labels'])
        self.assertEqual(len(instances[0]['components'][0]['data']['environments']), 1)
        self.assertEqual(len(instances[0]['components'][1]['data']['environments']), 1)
        self.assertIn("env1", instances[0]['components'][0]['data']['environments'])
        self.assertIn("Production", instances[0]['components'][1]['data']['environments'])

        self.assertEqual(len(instances[0]['relations']), 1)
        self.assertNotIn('labels', instances[0]['relations'][0]['data'])

    @mock.patch('codecs.open',
                side_effect=lambda location, mode, encoding: MockFileReader(location, {
                    'component.csv': ['id,name,type,environments', '1,name1,type1,"env1,env2"', '2,name2,type2,'],
                    'relation.csv': ['sourceid,targetid,type', '1,2,type']}))
    def test_topology_with_multiple_environments(self, mock):
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
        self.assertEqual(len(instances[0]['components'][0]['data']['labels']), 2)
        self.assertEqual(len(instances[0]['components'][1]['data']['labels']), 2)
        self.assertIn("csv.component:component.csv", instances[0]['components'][0]['data']['labels'])
        self.assertIn("csv.relation:relation.csv", instances[0]['components'][0]['data']['labels'])
        self.assertEqual(len(instances[0]['components'][0]['data']['environments']), 2)
        self.assertEqual(len(instances[0]['components'][1]['data']['environments']), 1)
        self.assertIn("env1", instances[0]['components'][0]['data']['environments'])
        self.assertIn("env2", instances[0]['components'][0]['data']['environments'])
        self.assertIn("Production", instances[0]['components'][1]['data']['environments'])

        self.assertEqual(len(instances[0]['relations']), 1)
        self.assertNotIn('labels', instances[0]['relations'][0]['data'])

    @mock.patch('codecs.open', side_effect=lambda location, mode, encoding: MockFileReader(location, {
        'component.csv': ['NOID,name,type'],
        'relation.csv': []}))
    def test_missing_component_id_field(self, mock):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'components_file': 'component.csv',
                    'relations_file': 'relations.csv',
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
                    'relations_file': 'relations.csv',
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
    def test_missing_component_type_field(self, mock):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'components_file': 'component.csv',
                    'relations_file': 'relations.csv',
                    'delimiter': ','
                }
            ]
        }

        with self.assertRaises(CheckException) as context:
            self.run_check(config)

        self.assertEquals('CSV header type not found in component csv.', str(context.exception))

    @mock.patch('codecs.open', side_effect=lambda location, mode, encoding: MockFileReader(location, {
        'components.csv': ['id,name,type', 'id1,name1,type1', ''],
        'relations.csv': ['sourceid,targetid,type']}))
    def test_handle_empty_component_line(self, mock):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'components_file': 'components.csv',
                    'relations_file': 'relations.csv',
                    'delimiter': ','
                }
            ]
        }

        self.run_check(config)
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(len(instances[0]['components']), 1)
        self.assertEqual(len(instances[0]['relations']), 0)

    @mock.patch('codecs.open', side_effect=lambda location, mode, encoding: MockFileReader(location, {
        'components.csv': ['id,name,type,othervalue', 'id1,name1,type1,othervalue', 'id2,name2,type2'],
        'relations.csv': ['sourceid,targetid,type']}))
    def test_handle_incomplete_component_line(self, mock):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'components_file': 'components.csv',
                    'relations_file': 'relations.csv',
                    'delimiter': ','
                }
            ]
        }

        self.run_check(config)
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(len(instances[0]['components']), 1)
        self.assertEqual(len(instances[0]['relations']), 0)

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

    @mock.patch('codecs.open', side_effect=lambda location, mode, encoding: MockFileReader(location, {
        'components.csv': ['id,name,type', 'id1,name1,type1', 'id2,name2,type2'],
        'relations.csv': ['sourceid,targetid,type', 'id1,id2,uses', '']}))
    def test_handle_empty_relation_line(self, mock):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'components_file': 'components.csv',
                    'relations_file': 'relations.csv',
                    'delimiter': ','
                }
            ]
        }

        self.run_check(config)
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(len(instances[0]['components']), 2)
        self.assertEqual(len(instances[0]['relations']), 1)

    @mock.patch('codecs.open', side_effect=lambda location, mode, encoding: MockFileReader(location, {
        'components.csv': ['id,name,type', 'id1,name1,type1', 'id2,name2,type2'],
        'relations.csv': ['sourceid,targetid,type,othervalue', 'id1,id2,uses,othervalue', 'id2,id3,uses']}))
    def test_handle_incomplete_relation_line(self, mock):
        config = {
            'init_config': {},
            'instances': [
                {
                    'type': 'csv',
                    'components_file': 'components.csv',
                    'relations_file': 'relations.csv',
                    'delimiter': ','
                }
            ]
        }

        self.run_check(config)
        instances = self.check.get_topology_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(len(instances[0]['components']), 2)
        self.assertEqual(len(instances[0]['relations']), 1)
