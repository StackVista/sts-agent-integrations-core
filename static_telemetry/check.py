"""
    StackState.
    Static telemetry.
"""

from checks import AgentCheck, CheckException
import codecs
import csv
import time


class StaticTelemetry(AgentCheck):
    SERVICE_CHECK_NAME = "StaticTopology"

    def check(self, instance):
        if 'type' not in instance:
            raise CheckException('Static telemetry instance missing "type" value.')
        if 'events_file' not in instance:
            raise CheckException('Static telemetry instance missing "events_file" value.')
        if 'delimiter' not in instance:
            raise CheckException('Static telemetry instance missing "delimiter" value.')

        timestamp_field = None
        if 'timestamp_field' in instance:
            timestamp_field = instance['timestamp_field']
            self.log.debug("Using field %s for timestamp." % timestamp_field)

        if instance['type'].lower() == "csv":
            events_file = instance['events_file']
            delimiter = instance['delimiter']
            self.handle_events_csv(events_file, delimiter, timestamp_field)
        else:
            raise CheckException('Static telemetry instance only supports type CSV.')

    def handle_events_csv(self, filelocation, delimiter, timestamp_field=None):
        self.log.debug("Processing events CSV file %s." % filelocation)

        with codecs.open(filelocation, mode='r', encoding="utf-8-sig") as csvfile:
            reader = csv.reader(csvfile, delimiter=delimiter, quotechar='"')

            header_row = next(reader, None)
            if header_row is None:
                raise CheckException("Events CSV file is empty.")
            self.log.debug("Detected events header: %s" % str(header_row))

            if len(header_row) == 1:
                self.log.warn("Detected one field in header, is the delimiter set properly?")
                self.log.warn("Detected header: %s" % str(header_row))

            # mandatory fields (timestamp field only at this moment if present)
            mandatory_fields = [] if not timestamp_field else [timestamp_field]
            for mandatory_field in mandatory_fields:
                if mandatory_field not in header_row:
                    raise CheckException('CSV header "%s" not found in csv.' % mandatory_field)

            optionals = ['host', 'event_type', 'msg_text', 'msg_title', 'alert_type', 'source_type_name', 'persist']

            for row in reader:
                self.log.debug("Processing row: %s" % row)
                data = dict(zip(header_row, row))

                # tags to contain all keys excluding the optional fields
                tags = ["%s:%s" % (key, value) for (key, value) in data.items() if key not in optionals]

                # event's timestamp
                timestamp = time.time()
                if timestamp_field:  # override timestamp if the field is set in the yaml
                    if not data[timestamp_field]:
                        self.log.error("Timestamp field is empty, skipping row")
                        continue
                    else:
                        try:
                            timestamp = float(data[timestamp_field])
                        except ValueError:
                            self.log.error("Unable to parse epoch timestamp to a float, skipping row")
                            continue

                event = {
                    "timestamp": timestamp,
                    "tags": tags
                }

                # optional event fields
                for optional in optionals:
                    if optional in data:
                        event[optional] = data[optional]

                self.event(event)
