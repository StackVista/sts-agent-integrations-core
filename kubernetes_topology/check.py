"""
    StackState.
    Kubernetes topology extraction

    Collects topology from k8s API.
"""

from collections import defaultdict

# 3rd party
import requests

# project
from checks import AgentCheck
from utils.kubernetes import KubeUtil

class KubernetesTopology(AgentCheck):
    INSTANCE_TYPE = "kubernetes"
    SERVICE_CHECK_NAME = "kubernetes.topology_information"
    DEFAULT_KUBERNETES_URL = "http://kubernetes"

    def __init__(self, name, init_config, agentConfig, instances=None):
        if instances is not None and len(instances) > 1:
            raise Exception('Kubernetes check only supports one configured instance.')

        AgentCheck.__init__(self, name, init_config, agentConfig, instances)

        inst = instances[0] if instances is not None else None
        self.kubeutil = KubeUtil(init_config=init_config, instance=inst, use_kubelet=False)

        if not self.kubeutil.init_success:
            if self.kubeutil.left_init_retries > 0:
                self.log.warning("Kubelet client failed to initialized for now, pausing the Kubernetes check.")
            else:
                raise Exception('Unable to initialize Kubelet client. Try setting the host parameter. The Kubernetes check failed permanently.')

    def check(self, instance):
        instance_key = {'type': self.INSTANCE_TYPE, 'url': self.kubeutil.kubernetes_api_root_url}
        msg = None
        status = None
        url = self.kubeutil.kubernetes_api_url

        if not url:
            raise Exception('Unable to reach kubernetes. Try setting the master_name and master_port parameter.')

        self.start_snapshot(instance_key)
        try:
            self._extract_topology(instance_key)
        except requests.exceptions.Timeout as e:
            # If there's a timeout
            msg = "%s seconds timeout when hitting %s" % (self.kubeutil.timeoutSeconds, url)
            status = AgentCheck.CRITICAL
        except Exception as e:
            self.log.warning('kubernetes topology check %s failed: %s' % (url, str(e)))
            msg = str(e)
            status = AgentCheck.CRITICAL
        finally:
            if status is AgentCheck.CRITICAL:
                self.service_check(self.SERVICE_CHECK_NAME, status, message=msg)

        self.stop_snapshot(instance_key)

    def _extract_topology(self, instance_key):
        self._extract_services(instance_key)
        self._extract_nodes(instance_key)
        self._extract_pods(instance_key)
        self._link_pods_to_services(instance_key)
        self._extract_deployments(instance_key)

    def _extract_services(self, instance_key):
        for service in self.kubeutil.retrieve_services_list()['items']:
            data = dict()
            data['type'] = service['spec']['type']
            data['namespace'] = service['metadata']['namespace']
            data['ports'] = service['spec'].get('ports', [])
            data['labels'] = self._make_labels(service['metadata'])
            if 'clusterIP' in service['spec'].keys():
                data['cluster_ip'] = service['spec']['clusterIP']
            self.component(instance_key, service['metadata']['name'], {'name': 'KUBERNETES_SERVICE'}, data)

    def _extract_nodes(self, instance_key):
        for node in self.kubeutil.retrieve_nodes_list()['items']:
            status_addresses = node['status'].get("addresses",[])
            addresses = {item['type']: item['address'] for item in status_addresses}

            data = dict()
            data['labels'] = self._make_labels(node['metadata'])
            data['internal_ip'] = addresses.get('InternalIP', None)
            data['legacy_host_ip'] = addresses.get('LegacyHostIP', None)
            data['hostname'] = addresses.get('Hostname', None)
            data['external_ip'] = addresses.get('ExternalIP', None)

            self.component(instance_key, node['metadata']['name'], {'name': 'KUBERNETES_NODE'}, data)

    def _extract_deployments(self, instance_key):
        for deployment in self.kubeutil.retrieve_deployments_list()['items']:
            data = dict()
            externalId = "deployment: %s" % deployment['metadata']['name']
            data['namespace'] = deployment['metadata']['namespace']
            data['name'] = deployment['metadata']['name']
            data['labels'] = self._make_labels(deployment['metadata'])

            deployment_template = deployment['spec']['template']
            if deployment_template and deployment_template['metadata']['labels'] and len(deployment_template['metadata']['labels']) > 0:
                data['template_labels'] = self._make_labels(deployment_template['metadata'])
                replicasets = self.kubeutil.retrieve_replicaset_filtered_list(deployment['metadata']['namespace'], deployment_template['metadata']['labels'])
                if replicasets['items']:
                    for replicaset in replicasets['items']:
                        self.relation(instance_key, externalId, replicaset['metadata']['name'], {'name': 'CREATED'}, dict())

            self.component(instance_key, externalId, {'name': 'KUBERNETES_DEPLOYMENT'}, data)

    def _extract_pods(self, instance_key):
        replicasets_to_pods = defaultdict(list)
        replicaset_to_data = dict()
        for pod in self.kubeutil.retrieve_master_pods_list()['items']:
            data = dict()
            pod_name = pod['metadata']['name']
            data['uid'] = pod['metadata']['uid']
            data['namespace'] = pod['metadata']['namespace']
            data['labels'] = self._make_labels(pod['metadata'])

            self.component(instance_key, pod_name, {'name': 'KUBERNETES_POD'}, data)

            relation_data = dict()
            if 'nodeName' in pod['spec']:
                self.relation(instance_key, pod_name, pod['spec']['nodeName'], {'name': 'PLACED_ON'}, relation_data)

            if 'containerStatuses' in pod['status'].keys():
                if 'nodeName' in pod['spec']:
                    pod_node_name = pod['spec']['nodeName']

                    if 'podIP' in pod['status']:
                        pod_ip = pod['status']['podIP']
                    else:
                        pod_ip = None

                    if 'hostIP' in pod['status']:
                        host_ip = pod['status']['hostIP']
                    else:
                        host_ip = None

                    self._extract_containers(instance_key, pod_name, pod_ip, host_ip, pod_node_name, pod['metadata']['namespace'], pod['status']['containerStatuses'])

            if 'ownerReferences' in pod['metadata'].keys():
                for reference in pod['metadata']['ownerReferences']:
                    if reference['kind'] == 'ReplicaSet':
                        data = dict()
                        data['name'] = pod_name
                        replicasets_to_pods[reference['name']].append(data)
                        if reference['name'] not in replicaset_to_data:
                            replicaset_data = dict()
                            replicaset_data['labels'] = self._make_labels(pod['metadata'])
                            replicaset_data['namespace'] = pod['metadata']['namespace']
                            replicaset_to_data[reference['name']] = replicaset_data

        for replicaset_name in replicasets_to_pods:
            self.component(instance_key, replicaset_name, {'name': 'KUBERNETES_REPLICASET'}, replicaset_to_data[replicaset_name])
            for pod in replicasets_to_pods[replicaset_name]:
                self.relation(instance_key, replicaset_name, pod['name'], {'name': 'CONTROLS'}, dict())

    def _extract_containers(self, instance_key, pod_name, pod_ip, host_ip, host_name, namespace, statuses):
        for containerStatus in statuses:
            container_id = containerStatus['containerID']
            data = dict()
            data['pod_ip'] = pod_ip
            data['host_ip'] = host_ip
            data['namespace'] = namespace
            data['labels'] = ["namespace:%s" % namespace]
            data['docker'] = {
                'image': containerStatus['image'],
                'container_id': container_id
            }
            self.component(instance_key, container_id, {'name': 'KUBERNETES_CONTAINER'}, data)

            relation_data = dict()
            self.relation(instance_key, pod_name, container_id, {'name': 'CONSISTS_OF'}, relation_data)
            self.relation(instance_key, container_id, host_name, {'name': 'HOSTED_ON'}, relation_data)

    def _link_pods_to_services(self, instance_key):
        for endpoint in self.kubeutil.retrieve_endpoints_list()['items']:
            service_name = endpoint['metadata']['name']
            if 'subsets' in endpoint:
                for subset in endpoint['subsets']:
                    if 'addresses' in subset:
                        for address in subset['addresses']:
                            if 'targetRef' in address.keys() and address['targetRef']['kind'] == 'Pod':
                                data = dict()
                                pod_name = address['targetRef']['name']
                                self.relation(instance_key, service_name, pod_name, {'name': 'EXPOSES'}, data)

    @staticmethod
    def extract_metadata_labels(metadata):
        """
        Extract labels from metadata section coming from the kubelet API.
        """
        kube_labels = defaultdict(list)
        name = metadata.get("name")
        namespace = metadata.get("namespace")
        labels = metadata.get("labels")
        if name and labels:
            if namespace:
                key = "%s/%s" % (namespace, name)
            else:
                key = name

            for k, v in labels.iteritems():
                kube_labels[key].append(u"%s:%s" % (k, v))

        return kube_labels

    def _make_labels(self, metadata):
        original_labels = self._flatten_dict(KubernetesTopology.extract_metadata_labels(metadata=metadata))
        if 'namespace' in metadata:
            original_labels.append("namespace:%s" % metadata['namespace'])
        return original_labels

    def _flatten_dict(self, dict_of_list):
        from itertools import chain
        return sorted(set(chain.from_iterable(dict_of_list.itervalues())))
