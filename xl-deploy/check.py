from checks import AgentCheck

import requests
import untangle
import datetime
import pytz
import urllib
import re
import time

from utils.persistable_store import PersistableStore

INSTANCE_TYPE = "xl-deploy"
SERVICE_CHECK_NAME = "xl-deploy.topology_information"
DEFAULT_XLD_URL = "http://xl-deploy:4516"
EVENT_TYPE = "deployment"
PERSISTENCE_CHECK_NAME = "xl-deploy"


class XlDeployClient:

    def __init__(self, url, un, pw):
        self._url = url
        self._un = un
        self._pw = pw
        print "Accessing XL Deploy at {} with user {}".format(self._url, self._un)

    def get(self, url):
        response = requests.get(url, auth=(self._un, self._pw))

        o = untangle.parse(response.text)
        return o

    def query(self, url, most_recent_check):
        if most_recent_check is None:
            return self.get(url)
        else:
            return self.get(url + '&lastModifiedAfter=' + urllib.quote(most_recent_check))

    def host_query(self, most_recent_check):
        return self.query(self._url + '/query?type=udm.Container', most_recent_check)

    def deployment_query(self, most_recent_check):
        return self.query(self._url + '/query?type=udm.DeployedApplication', most_recent_check)

    def ci_query(self, ci, most_recent_check = None):
        return self.query(self._url + '/ci/' + ci, most_recent_check)


class XlDeploy(AgentCheck):

    xld_client = None  # XL-Deploy connection client
    _persistable_store = None  # storage for check/instance data
    recent_check_timestamp = None  # poll XL-Deploy for changes after this timestamp

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)
        self.assumed_url = {}

    def get_child_value(self, node, child_name):
        return self.get_child_node(node, child_name).cdata

    def get_child_attribute(self, node, child_name, attr_name):
        return self.get_child_node(node, child_name)[attr_name]

    def get_child_node(self, node, child_name):
        return filter((lambda x: x._name == child_name), node.children)[0]

    def collect_data(self, container, data):
        for ci in container.children:
            if ci._name == 'password' or ci._name == 'tags' or ci._name == 'contextRoot':
                continue

            data[ci._name] = self.get_child_value(container, ci._name)
            if ci._name == 'deployable':
                # Capture the version of the deployable
                match = re.search('\/([^\/]+)\/([0-9\.]+)\/', ci["ref"])
                data["application"] = match.group(1)
                data["version"] = match.group(2)

    def topology_from_ci(self, instance_key, ci, environment=None):
        cont = self.xld_client.ci_query(ci).children[0]

        data = dict()
        self.collect_data(cont, data)
        data["ci_type"] = cont._name
        if environment is not None:
            data["environment"] = environment

        ci_type = {'name': cont._name }
        self.component(instance_key, cont["id"], ci_type, data)

        # Link the container to it's direct parent
        parts = cont["id"].split('/')
        if len(parts) > 2:
            parts = parts[:-1]
            relation_data = dict()
            self.relation(instance_key, cont["id"], '/'.join(parts), {'name': 'RUNS_ON'}, relation_data)

    def get_topology(self, instance_key, most_recent_check):
        o = self.xld_client.host_query(most_recent_check)

        cis = []
        for ci in o.list.children:
            cis.append(ci["ref"])

        cis.sort()

        for ci in cis:
            self.topology_from_ci(instance_key, ci)

    def handle_deployment(self, instance_key, deployment):
        for m in self.get_child_node(deployment, "deployeds").children:
            self.handle_deployed(instance_key, deployment, m["ref"])

    def deployment_event(self, deployed_id, environment_name, application_name, version_number, event_ts):
        title = 'Deployment of {} {}'.format(application_name, version_number)
        msg_body = title

        dd_event = {
            'timestamp': event_ts,
            'host': self.hostname,
            'event_type': EVENT_TYPE,
            'msg_title': title,
            'msg_text': msg_body,
            'source_type_name': EVENT_TYPE,
            'api_key': self.agentConfig['api_key'],
            'aggregation_key': EVENT_TYPE,
            'tags': [
                'affects-' + deployed_id,
                'application-' + application_name,
                'version-' + version_number,
                'environment-' + environment_name
            ]
        }

        self.event(dd_event)

    def handle_deployed(self, instance_key, deployment, deployed_id):
        # A deployed id looks like this: Infrastructure/10.0.0.1/tc-server/vh1/zookeeper
        # This is broken down into three parts:
        #   - a component (zookeeper)
        #   - a relation to the container it is on (Infrastructure/10.0.0.1/tc-server/vh1)
        #   - a deployment event on that component

        match = re.search('\/([^\/]+)\/([^\/]+)$', deployment["id"])
        environment_name = match.group(1)
        application_name = match.group(2)
        match = re.search('\/([^\/]+)$', self.get_child_attribute(deployment, "version", "ref"))
        version_number = match.group(1)

        # timestamp = str(int(dateutil.parser.parse(deployment["last-modified-at"]).strftime('%s')) * 1000)
        timestamp = int(time.time())

        self.topology_from_ci(instance_key, deployed_id, environment_name)
        self.deployment_event(deployed_id, environment_name, application_name, version_number, timestamp)

    def get_deployments(self, instance_key, most_recent_check):
        o = self.xld_client.deployment_query(most_recent_check)

        # Iterate over deployments
        for ci in o.list.children:
                # Fetch deployment
                depl = self.xld_client.ci_query(ci["ref"]).children[0]
                self.handle_deployment(instance_key, depl)

    def load_status(self):
        """
        load recent timestamp to use in polling for new data.
        :return: nothing
        """
        self._persistable_store.load_status()
        if self._persistable_store['recent_timestamp'] is None:
            self._persistable_store['recent_timestamp'] = "1970-01-01T00:00:00.000000+00:00"

    def commit_succeeded(self, instance):
        """
        override from AgentCheck, commit succeeded to this point, we'll store the timestamp for next instance run
        :param instance: check instance
        :return: boolean indicating commit has succeeded
        """
        self._persistable_store['recent_timestamp'] = self.current_timestamp()
        self._persistable_store.commit_status()
        return True

    def current_timestamp(self):
        return datetime.datetime.now(tz=pytz.utc).isoformat()

    def commit_failed(self, instance):
        """
        Upon failure we do not commit the new timestamp such that it will be retried in a later instance check run.
        """
        pass

    def check(self, instance):
        url = instance['url']
        user = instance['user']
        password = instance['pass']
        instance_key = {'type': INSTANCE_TYPE, 'url': url}

        self.xld_client = XlDeployClient(url, user, password)

        self._persistable_store = PersistableStore(PERSISTENCE_CHECK_NAME, url)
        self.load_status()

        most_recent_check = self._persistable_store['recent_timestamp']

        self.get_topology(instance_key, most_recent_check)
        self.get_deployments(instance_key, most_recent_check)
