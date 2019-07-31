"""
    StackState.
    Static topology extraction
"""

# 3rd party
import csv
import codecs

from checks import AgentCheck, CheckException


class StaticTopology(AgentCheck):
    SERVICE_CHECK_NAME = "StaticTopology"

    def check(self, instance):
        if 'components_file' not in instance:
            raise CheckException('Static topology instance missing "components_file" value.')
        if 'type' not in instance:
            raise CheckException('Static topology instance missing "type" value.')

        instance_tags = instance['tags'] if 'tags' in instance else []

        if instance['type'].lower() == "csv":
            component_file = instance['components_file']
            relation_file = instance['relations_file'] if 'relations_file' in instance else None
            delimiter = instance['delimiter']
            instance_key = {"type": "StaticTopology", "url": component_file}
            self.start_snapshot(instance_key)
            self.handle_component_csv(instance_key, component_file, delimiter, instance_tags)
            if relation_file:
                self.handle_relation_csv(instance_key, relation_file, delimiter, instance_tags)
            self.stop_snapshot(instance_key)
        else:
            raise CheckException('Static topology instance only supports type CSV.')

    def handle_component_csv(self, instance_key, filelocation, delimiter, instance_tags):
        self.log.debug("Processing component CSV file %s." % filelocation)

        COMPONENT_ID_FIELD = 'id'
        COMPONENT_TYPE_FIELD = 'type'
        COMPONENT_NAME_FIELD = 'name'

        with codecs.open(filelocation, mode='r', encoding="utf-8-sig") as csvfile:
            reader = csv.reader(csvfile, delimiter=delimiter, quotechar='"')

            header_row = next(reader, None)
            if header_row is None:
                raise CheckException("Component CSV file is empty.")
            self.log.debug("Detected component header: %s" % str(header_row))

            if len(header_row) == 1:
                self.log.warn("Detected one field in header, is the delimiter set properly?")
                self.log.warn("Detected component header: %s" % str(header_row))

            # mandatory fields
            for field in (COMPONENT_ID_FIELD, COMPONENT_NAME_FIELD, COMPONENT_TYPE_FIELD):
                if field not in header_row:
                    raise CheckException('CSV header %s not found in component csv.' % field)
            id_idx = header_row.index(COMPONENT_ID_FIELD)
            type_idx = header_row.index(COMPONENT_TYPE_FIELD)

            for row in reader:
                data = dict(zip(header_row, row))

                # label processing
                labels = data.get('labels', "")
                labels = labels.split(',') if labels else []
                labels.extend(instance_tags)
                data['labels'] = labels

                # environment processing
                environments = data.get('environments', "Production")
                # environments column may be in the row but may be empty/unspecified for that row, defaulting to Production
                environments = environments.split(',') if environments else ["Production"]
                data['environments'] = environments

                # identifiers processing
                identifiers = data.get('identifiers', "")
                # identifiers column may be in the row but may be empty/unspecified for that row, defaulting
                identifiers = identifiers.split(',') if identifiers else []
                data['identifiers'] = identifiers

                self.component(instance_key=instance_key, id=row[id_idx], type={"name": row[type_idx]}, data=data)

    def handle_relation_csv(self, instance_key, filelocation, delimiter, instance_tags):
        self.log.debug("Processing relation CSV file %s." % filelocation)

        RELATION_SOURCE_ID_FIELD = 'sourceid'
        RELATION_TARGET_ID_FIELD = 'targetid'
        RELATION_TYPE_FIELD = 'type'

        with codecs.open(filelocation, mode='r', encoding="utf-8-sig") as csvfile:
            reader = csv.reader(csvfile, delimiter=delimiter, quotechar='|')

            header_row = next(reader, None)
            if header_row is None:
                raise CheckException("Relation CSV file is empty.")
            self.log.debug("Detected relation header: %s" % str(header_row))

            # mandatory fields
            for field in (RELATION_SOURCE_ID_FIELD, RELATION_TARGET_ID_FIELD, RELATION_TYPE_FIELD):
                if field not in header_row:
                    raise CheckException('CSV header %s not found in relation csv.' % field)
            source_id_idx = header_row.index(RELATION_SOURCE_ID_FIELD)
            target_id_idx = header_row.index(RELATION_TARGET_ID_FIELD)
            type_idx = header_row.index(RELATION_TYPE_FIELD)

            for row in reader:
                data = dict(zip(header_row, row))
                data['labels'] = instance_tags
                self.relation(instance_key=instance_key,
                              source_id=row[source_id_idx],
                              target_id=row[target_id_idx],
                              type={"name": row[type_idx]},
                              data=data)
