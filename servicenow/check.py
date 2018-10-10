# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib

# 3rd party
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3 import disable_warnings

# Disable https warnings
disable_warnings(InsecureRequestWarning)

# project
from checks import AgentCheck, CheckException

EVENT_TYPE = SOURCE_TYPE_NAME = 'servicenow'


class ServicenowCheck(AgentCheck):

    INSTANCE_TYPE = "servicenow_cmdb"
    SERVICE_CHECK_NAME = "servicenow.cmdb.topology_information"

    def check(self, instance):
        if 'url' not in instance:
            raise Exception('ServiceNow CMDB topology instance missing "url" value.')
        if 'basic_auth' not in instance:
            raise Exception('ServiceNow CMDB topology instance missing "basic_auth" value.')

        basic_auth = instance['basic_auth']
        if 'user' not in basic_auth:
            raise Exception('ServiceNow CMDB topology instance missing "basic_auth.user" value.')
        if 'password' not in basic_auth:
            raise Exception('ServiceNow CMDB topology instance missing "basic_auth.password" value.')

        relation_types = {}

        basic_auth_user = basic_auth['user']
        basic_auth_password = basic_auth['password']
        auth = (basic_auth_user, basic_auth_password)

        base_url = instance['url']
        batch_size = instance['default_batch_size']
        instance_key = {"type": self.INSTANCE_TYPE, "url": base_url}
        instance_tags = instance.get('tags', [])

        default_timeout = self.init_config.get('default_timeout', 5)
        timeout = float(instance.get('timeout', default_timeout))

        params = {"service_check_done": False, "batch_size": batch_size, "relation_types": relation_types,
                  "instance_key": instance_key, "instance_tags": instance_tags, "base_url": base_url,
                  "auth": auth, "timeout": timeout}

        self._process_and_cache_relation_types(params)
        self.start_snapshot(instance_key)
        self._process_components(params)
        self._process_component_relations(params)
        self.stop_snapshot(instance_key)

    def _collect_components(self, params):
        """
        collect components from ServiceNow CMDB's cmdb_ci table
        (API Doc- https://developer.servicenow.com/app.do#!/rest_api_doc?v=london&id=r_TableAPI-GET)

        :return: dict, raw response from CMDB
        """

        base_url = params['base_url']
        auth = params['auth']
        timeout = params['timeout']
        url = base_url + '/api/now/table/cmdb_ci?sysparm_fields=name,sys_id,sys_class_name,sys_created_on'

        return self._get_json(params, url, timeout, auth)

    def _process_components(self, params):
        """
        process components fetched from CMDB
        :return: nothing
        """
        instance_tags = params['instance_tags']
        instance_key = params['instance_key']

        state = self._collect_components(params)

        for component in state['result']:
            id = component['sys_id']
            type = {
                "name": component['sys_class_name']
            }
            data = {
                "name": component['name'].strip(),
                "tags": instance_tags
            }

            self.component(instance_key, id, type, data)

    def _collect_relation_types(self, params):
        """
        collects relations from CMDB
        :return: dict, raw response from CMDB
        """

        base_url = params['base_url']
        timeout = params['timeout']
        auth = params['auth']
        url = base_url + '/api/now/table/cmdb_rel_type?sysparm_fields=sys_id,parent_descriptor'

        return self._get_json(params, url, timeout, auth)

    def _process_and_cache_relation_types(self, params):
        """
        collect available relations from cmdb_rel_ci and cache them in self.relation_types dict.
        :return: nothing
        """

        relation_types = params['relation_types']
        state = self._collect_relation_types(params)

        for relation in state['result']:
            id = relation['sys_id']
            parent_descriptor = relation['parent_descriptor']
            relation_types[id] = parent_descriptor

    def _collect_component_relations(self, params, offset, batch_size):
        """
        collect relations between components from cmdb_rel_ci and publish these in batches.
        """
        base_url = params['base_url']
        timeout = params['timeout']
        auth = params['auth']
        url = base_url + '/api/now/table/cmdb_rel_ci?sysparm_fields=parent,type,child'

        return self._get_json_batch(params, url, offset, batch_size, timeout, auth)

    def _process_component_relations(self, params):
        batch_size = params['batch_size']
        offset = 0
        relation_types = params['relation_types']
        instance_tags = params['instance_tags']
        instance_key = params['instance_key']

        completed = False
        while not completed:
            state = self._collect_component_relations(params, offset, batch_size)['result']

            for relation in state:

                parent_sys_id = relation['parent']['value']
                child_sys_id = relation['child']['value']
                type_sys_id = relation['type']['value']

                relation_type = {
                    "name": relation_types[type_sys_id]
                }
                data = {
                    "tags": instance_tags
                }

                self.relation(instance_key, parent_sys_id, child_sys_id, relation_type, data)

            completed = len(state) < batch_size
            offset += batch_size

    def _get_json_batch(self, params, url, offset, batch_size, timeout, auth):
        limit_args = "&sysparm_query=ORDERBYsys_created_on&sysparm_offset=%i&sysparm_limit=%i" % (offset, batch_size)
        limited_url = url + limit_args
        return self._get_json(params, limited_url, timeout, auth)

    def _get_json(self, params, url, timeout, auth=None, verify=True):
        tags = ["url:%s" % url]
        msg = None
        status = None
        resp = None
        try:
            resp = requests.get(url, timeout=timeout, auth=auth, verify=verify)
            if resp.status_code != 200:
                status = AgentCheck.CRITICAL
                msg = "Got %s when hitting %s" % (resp.status_code, url)
            else:
                status = AgentCheck.OK
                msg = "ServiceNow CMDB instance detected at %s " % url
        except requests.exceptions.Timeout as e:
            # If there's a timeout
            msg = "%s seconds timeout when hitting %s" % (timeout, url)
            status = AgentCheck.CRITICAL
        except Exception as e:
            msg = str(e)
            status = AgentCheck.CRITICAL
        finally:
            if not params['service_check_done'] or status is AgentCheck.CRITICAL:
                self.make_service_check(params, status, tags=tags, msg=msg)

        if resp.encoding is None:
            resp.encoding = 'UTF8'

        return resp.json()

    def make_service_check(self, params, status, tags, msg):
        params['service_check_done'] = True
        if status is AgentCheck.OK:
            self.service_check(self.SERVICE_CHECK_NAME, status, tags=tags,
                               message=msg)
        if status is AgentCheck.CRITICAL:
            self.service_check(self.SERVICE_CHECK_NAME, status, tags=tags,
                               message=msg)
            raise CheckException("Cannot connect to ServiceNow CMDB, please check your configuration.")