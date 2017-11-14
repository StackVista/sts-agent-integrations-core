from checks import AgentCheck

import requests
import untangle
import datetime
import pytz
import urllib
import re

INSTANCE_TYPE = "xl-deploy"
SERVICE_CHECK_NAME = "xl-deploy.topology_information"
DEFAULT_XLD_URL = "http://xl-deploy:4516"
EVENT_TYPE = "deployment"

class XlDeployClient:

    def __init__(self, url, un, pw):
        self._url = url
        self._un = un
        self._pw = pw
        print "Accessing XL Deploy at {} with user {}".format(self._url, self._un)

    def get(self, url):
        # print url
        response = requests.get(url, auth=(self._un, self._pw))
        print response.text

        o = untangle.parse(response.text)
        return o

    def query(self, url, most_recent_check):
        if (most_recent_check == None):
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

    xld_client = None

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
            if (ci._name == 'password'):
                continue

            data[ci._name] = self.get_child_value(container, ci._name)
            if (ci._name == 'deployable'):
                # Capture the version of the deployable
                match = re.search('\/([^\/]+)\/([0-9\.]+)\/', ci["ref"])
                data["application"] = match.group(1)
                data["version"] = match.group(2)

    def topology_from_ci(self, instance_key, ci, environment = None):
        cont = self.xld_client.ci_query(ci).children[0]

        data = dict()
        self.collect_data(cont, data)
        if (environment is not None):
            data["environment"] = environment

        self.component(instance_key, cont["id"], {'name': cont._name}, data)

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

    def get_most_recent_check_ts(self):
        most_recent_check_ts = "1970-01-01T00:00:00.000000+00:00"

        try:
            with open("/tmp/most_recent_check_ts.txt", "r") as f:
                most_recent_check_ts = f.read()
        except:
            print "Unable to open ts file"
        finally:
            pass

        print 'Most recent ts: ' + most_recent_check_ts
        return most_recent_check_ts

    def store_most_recent_check_ts(self):
        with open("/tmp/most_recent_check_ts.txt", "w") as out:
            out.write(datetime.datetime.now(tz=pytz.utc).isoformat())

    def handle_deployment(self, instance_key, deployment):
        for m in self.get_child_node(deployment, "deployeds").children:
            self.handle_deployed(instance_key, deployment, m["ref"])

    def deployment_event(self, deployed_id, environment_name, application_name, version_number, event_ts):
        title = 'Deployment of {} {}'.format(application_name, version_number)
        msg_body = title

        affects_tag = '"affects": "{}"'.format(deployed_id)
        app_tag = '"application": "{}"'.format(application_name)
        version_tag = '"version": "{}"'.format(version_number)
        env_tag = '"environment": "{}"'.format(environment_name)

        dd_event = {
            'timestamp': event_ts,
            'host': '10.0.0.1', # TODO change to instance host
            'event_type': EVENT_TYPE,
            'msg_title': title,
            'msg_text': msg_body,
            'source_type_name': EVENT_TYPE,
            'tags': '{{ {}, {}, {}, {} }}'.format(app_tag, version_tag, affects_tag, env_tag)
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

        self.topology_from_ci(instance_key, deployed_id, environment_name)
        self.deployment_event(deployed_id, environment_name, application_name, version_number, deployment["last-modified-at"])

    def get_deployments(self, instance_key, most_recent_check):
        o = self.xld_client.deployment_query(most_recent_check)

        # Iterate over deployments
        for ci in o.list.children:
                # Fetch deployment
                depl = self.xld_client.ci_query(ci["ref"]).children[0]
                self.handle_deployment(instance_key, depl)

    def check(self, instance):
        url = instance['url']
        user = instance['user']
        password = instance['pass']
        self.xld_client = XlDeployClient(url, user, password)
        instance_key = {'type': INSTANCE_TYPE, 'url': url}

        most_recent_check = self.get_most_recent_check_ts()

        self.get_topology(instance_key, most_recent_check)
        self.get_deployments(instance_key, most_recent_check)

        self.store_most_recent_check_ts()
