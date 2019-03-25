# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
import os

# 3p
from mock import Mock, MagicMock
from pyVmomi import vim  # pylint: disable=E0611
import simplejson as json

# datadog
from tests.checks.common import AgentCheckTest, Fixtures

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), 'ci')

class MockedMOR(Mock):
    """
    Helper, generate a mocked Managed Object Reference (MOR) from the given attributes.
    """
    def __init__(self, **kwargs):
        # Deserialize `spec`
        if 'spec' in kwargs:
            kwargs['spec'] = getattr(vim, kwargs['spec'])

        # Mocking
        super(MockedMOR, self).__init__(**kwargs)

        # Handle special attributes
        name = kwargs.get('name')
        is_labeled = kwargs.get('label', False)

        self.name = name
        self.parent = None
        self.customValue = []

        if is_labeled:
            self.customValue.append(Mock(value="StackStateMonitored"))


class MockedContainer(Mock):
    TYPES = [vim.Datacenter, vim.Datastore, vim.HostSystem, vim.VirtualMachine]

    def __init__(self, **kwargs):
        # Mocking
        super(MockedContainer, self).__init__(**kwargs)

        self.topology = kwargs.get('topology')
        self.view_idx = 0

    def container_view(self, topology_node, vimtype):
        view = []

        def get_child_topology(attribute):
            entity = getattr(topology_node, attribute)
            try:
                for child in entity:
                    child_topology = self.container_view(child, vimtype)
                    view.extend(child_topology)
            except TypeError:
                child_topology = self.container_view(entity, vimtype)
                view.extend(child_topology)

        if isinstance(topology_node, vimtype):
            view = [topology_node]

        if hasattr(topology_node, 'childEntity'):
            get_child_topology('childEntity')
        elif hasattr(topology_node, 'hostFolder'):
            get_child_topology('hostFolder')
        elif hasattr(topology_node, 'host'):
            get_child_topology('host')
        elif hasattr(topology_node, 'vm'):
            get_child_topology('vm')

        return view

    @property
    def view(self):
        view = self.container_view(self.topology, self.TYPES[self.view_idx])
        self.view_idx += 1
        self.view_idx = self.view_idx % len(self.TYPES)
        return view


def create_topology(topology_json):
    """
    Helper, recursively generate a vCenter topology from a JSON description.
    Return a `MockedMOR` object.

    Examples:
      ```
      topology_desc = "
        {
          "childEntity": [
            {
              "hostFolder": {
                "childEntity": [
                  {
                    "spec": "ClusterComputeResource",
                    "name": "compute_resource1"
                  }
                ]
              },
              "spec": "Datacenter",
              "name": "datacenter1"
            }
          ],
          "spec": "Folder",
          "name": "rootFolder"
        }
      "

      topo = create_topology(topology_desc)

      assert isinstance(topo, Folder)
      assert isinstance(topo.childEntity[0].name) == "compute_resource1"
      ```
    """
    def rec_build(topology_desc):
        """
        Build MORs recursively.
        """
        parsed_topology = {}

        for field, value in topology_desc.iteritems():
            parsed_value = value
            if isinstance(value, dict):
                parsed_value = rec_build(value)
            elif isinstance(value, list):
                parsed_value = [rec_build(obj) for obj in value]
            else:
                parsed_value = value
            parsed_topology[field] = parsed_value

        mor = MockedMOR(**parsed_topology)

        # set parent
        for field, value in topology_desc.iteritems():
            if isinstance(parsed_topology[field], list):
                for m in parsed_topology[field]:
                    if isinstance(m, MockedMOR):
                        m.parent = mor
            elif isinstance(parsed_topology[field], MockedMOR):
                parsed_topology[field].parent = mor

        return mor

    return rec_build(json.loads(Fixtures.read_file(topology_json, sdk_dir=FIXTURE_DIR)))



class TestvSphereUnit(AgentCheckTest):
    """
    Unit tests for vSphere AgentCheck.
    """
    CHECK_NAME = "vsphere"

    def assertMOR(self, instance, name=None, spec=None, tags=None, count=None, subset=False):
        """
        Helper, assertion on vCenter Manage Object References.
        """
        instance_name = instance['name']
        candidates = []

        if spec:
            mor_list = self.check.morlist_raw[instance_name][spec]
        else:
            mor_list = [mor for _, mors in self.check.morlist_raw[instance_name].iteritems() for mor in mors]

        for mor in mor_list:
            if name is not None and name != mor['hostname']:
                continue

            if spec is not None and spec != mor['mor_type']:
                continue

            if tags is not None:
                if subset:
                    if not set(tags).issubset(set(mor['tags'])):
                        continue
                elif set(tags) != set(mor['tags']):
                    continue

            candidates.append(mor)

        # Assertions
        if count:
            self.assertEquals(len(candidates), count)
        else:
            self.assertFalse(len(candidates))

    def setUp(self):
        """
        Initialize and patch the check, i.e.
        * disable threading
        * create a unique container for MORs independent of the instance key
        """
        # Initialize
        config = {}
        self.load_check(config)

        # Disable threading
        self.check.pool = Mock(apply_async=lambda func, args: func(*args))

        # Create a container for MORs
        self.check.morlist_raw = {}


    def test_exclude_host(self):
        """
        Exclude hosts/vms not compliant with the user's `*_include` configuration.
        """
        # Method to test
        is_excluded = self.check._is_excluded

        # Sample(s)
        include_regexes = {
            'host_include': "f[o]+",
            'vm_include': "f[o]+",
        }

        # OK
        included_host = MockedMOR(spec="HostSystem", name="foo")
        included_vm = MockedMOR(spec="VirtualMachine", name="foo")

        self.assertFalse(is_excluded(included_host, include_regexes, None))
        self.assertFalse(is_excluded(included_vm, include_regexes, None))

        # Not OK!
        excluded_host = MockedMOR(spec="HostSystem", name="bar")
        excluded_vm = MockedMOR(spec="VirtualMachine", name="bar")

        self.assertTrue(is_excluded(excluded_host, include_regexes, None))
        self.assertTrue(is_excluded(excluded_vm, include_regexes, None))

    def test_exclude_non_labeled_vm(self):
        """
        Exclude "non-labeled" virtual machines when the user configuration instructs to.
        """
        # Method to test
        is_excluded = self.check._is_excluded

        # Sample(s)
        include_regexes = None
        include_only_marked = True

        # OK
        included_vm = MockedMOR(spec="VirtualMachine", name="foo", label=True)
        self.assertFalse(is_excluded(included_vm, include_regexes, include_only_marked))

        # Not OK
        included_vm = MockedMOR(spec="VirtualMachine", name="foo")
        self.assertTrue(is_excluded(included_vm, include_regexes, include_only_marked))

    def test_mor_discovery(self):
        """
        Explore the vCenter infrastructure to discover hosts, virtual machines.

        Input topology:
            ```
            rootFolder
                - datacenter1
                  - compute_resource1
                      - host1                   # Filtered out
                      - host2
                - folder1
                    - datacenter2
                      - compute_resource2
                          - host3
                            - vm1               # Not labeled
                            - vm2               # Filtered out
                            - vm3               # Powered off
                            - vm4
            ```
        """
        # Method to test
        discover_mor = self.check._discover_mor

        # Samples
        instance = {'name': 'vsphere_mock'}
        vcenter_topology = create_topology('vsphere_topology.json')
        tags = [u"toto"]
        include_regexes = {
            'host_include': "host[2-9]",
            'vm_include': "vm[^2]",
        }
        include_only_marked = True

        # mock pyvmomi stuff
        view_mock = MockedContainer(topology=vcenter_topology)
        viewmanager_mock = MagicMock(**{'CreateContainerView.return_value': view_mock})
        content_mock = MagicMock(viewManager=viewmanager_mock)
        server_mock = MagicMock()
        server_mock.configure_mock(**{'RetrieveContent.return_value': content_mock})
        self.check._get_server_instance = MagicMock(return_value=server_mock)


        # Discover hosts and virtual machines
        discover_mor(instance, tags, include_regexes, include_only_marked)

        # Assertions: 1 labaled+monitored VM + 2 hosts + 2 datacenters.
        self.assertMOR(instance, count=7)

        # ... on hosts
        self.assertMOR(instance, spec="host", count=2)
        self.assertMOR(
            instance,
            name="host2", spec="host",
            tags=[
                u"toto", u"vsphere_folder:rootFolder", u"vsphere_datacenter:datacenter1",
                u"vsphere_compute:compute_resource1", u"vsphere_cluster:compute_resource1",
                u"vsphere_type:host"
            ]
        )
        self.assertMOR(
            instance,
            name="host3", spec="host",
            tags=[
                u"toto", u"vsphere_folder:rootFolder", u"vsphere_folder:folder1",
                u"vsphere_datacenter:datacenter2", u"vsphere_compute:compute_resource2",
                u"vsphere_cluster:compute_resource2", u"vsphere_type:host"
            ]
        )

        # ...on VMs
        self.assertMOR(instance, spec="vm")
        self.assertMOR(
            instance,
            name="vm4", spec="vm", subset=True,
            tags=[
                u"toto", u"vsphere_folder:folder1", u"vsphere_datacenter:datacenter2",
                u"vsphere_compute:compute_resource2",u"vsphere_cluster:compute_resource2",
                u"vsphere_host:host3", u"vsphere_type:vm"
            ]
        )


class TestVsphereTopo(AgentCheckTest):

    CHECK_NAME = "vsphere"

    def mock_content(self, vimtype):
        # summary object for datastore
        ds_summary = MockedMOR(accessible="true", capacity=987959765, type="VMFS",
                               url='/vmfs/volumes/54183927-04f91918-a72a-6805ca147c55')
        # hardware and config object needed for vms
        vm_config_hardware = MockedMOR(numCPU=1, memoryMB=4096)
        config = MockedMOR(guestId='ubuntu64Guest', guestFullName='Ubuntu Linux (64-bit)', hardware=vm_config_hardware)

        datastore = MockedMOR(spec='Datastore', _moId="54183927-04f91918-a72a-6805ca147c55", name="WDC1TB")
        virtualmachine = MockedMOR(spec="VirtualMachine", name="Ubuntu", datastore=[datastore], config=config)
        host = MockedMOR(spec="HostSystem", name="localhost.localdomain", datastore=[datastore], vm=[virtualmachine])
        computeresource = MockedMOR(spec="ComputeResource", name="localhost", datastore=[datastore], host=[host])
        clustercomputeresource = MockedMOR(spec="ClusterComputeResource", name="local",
                                           datastore=[datastore], host=[host])

        datacenter = MockedMOR(spec="Datacenter", name="da-Datacenter", _moId="54183347-04d231918",
                               hostFolder=MockedMOR(childEntity=[computeresource]), datastore=[datastore])

        if vimtype == 'vm':
            view_mock = MagicMock(view=[virtualmachine])
        elif vimtype == 'dc':
            view_mock = MagicMock(view=[datacenter])
        elif vimtype == 'ds':
            datastore.summary = ds_summary
            datastore.vm = [virtualmachine]
            view_mock = MagicMock(view=[datastore])
        elif vimtype == 'host':
            host.parent = computeresource
            view_mock = MagicMock(view=[host])
        elif vimtype == 'cluster':
            view_mock = MagicMock(view=[clustercomputeresource])
        else:
            view_mock = MagicMock(view=[computeresource])

        viewmanager_mock = MagicMock(**{'CreateContainerView.return_value': view_mock})
        content_mock = MagicMock(viewManager=viewmanager_mock)
        return content_mock

    def test_vsphere_vms(self):
        """
        Test if the vsphere_vms returns the VM list and labels
        """
        config = {}
        self.load_check(config)
        self.check._is_excluded = MagicMock(return_value=False)

        content_mock = self.mock_content("vm")
        obj_list = self.check._vsphere_vms(content_mock, "ESXi")

        self.assertEqual(len(obj_list), 1)
        self.assertEqual(obj_list[0]['hostname'], 'Ubuntu')

        # Check if labels are added
        self.assertTrue(obj_list[0]['topo_tags']["labels"])
        expected_name_label = obj_list[0]['topo_tags']["labels"][0]
        expected_guestid_label = obj_list[0]['topo_tags']["labels"][1]
        expected_numcpu_label = obj_list[0]['topo_tags']["labels"][3]
        expected_memory_label = obj_list[0]['topo_tags']["labels"][4]

        # Check if the labels are as expected
        self.assertEqual(expected_name_label, 'name:Ubuntu')
        self.assertEqual(expected_guestid_label, 'guestId:ubuntu64Guest')
        self.assertEqual(expected_numcpu_label, 'numCPU:1')
        self.assertEqual(expected_memory_label, 'memoryMB:4096')

    def test_vsphere_datacenters(self):
        """
        Test if the vsphere_datacenter returns the datacenter list
        """
        config = {}
        self.load_check(config)
        self.check._is_excluded = MagicMock(return_value=False)

        content_mock = self.mock_content("dc")
        obj_list = self.check._vsphere_datacenters(content_mock, "ESXi")

        self.assertEqual(len(obj_list), 1)
        self.assertEqual(type(obj_list[0]['topo_tags']['datastores']), list)
        self.assertEqual(obj_list[0]['topo_tags']['datastores'][0], 'WDC1TB')
        self.assertEqual(obj_list[0]['topo_tags']['name'], 'da-Datacenter')

    def test_vsphere_datastores(self):
        """
        Test if the vsphere_datastores returns the datastores list
        """
        config = {}
        self.load_check(config)
        self.check._is_excluded = MagicMock(return_value=False)

        content_mock = self.mock_content("ds")
        obj_list = self.check._vsphere_datastores(content_mock, "ESXi")

        self.assertEqual(len(obj_list), 1)
        self.assertEqual(obj_list[0]['topo_tags']['type'], 'VMFS')
        self.assertEqual(obj_list[0]['topo_tags']['accessible'], 'true')
        self.assertEqual(obj_list[0]['topo_tags']['name'], 'WDC1TB')
        self.assertEqual(obj_list[0]['topo_tags']['url'], '/vmfs/volumes/54183927-04f91918-a72a-6805ca147c55')

    def test_vsphere_hosts(self):
        """
        Test if the vsphere_hosts returns the hosts list
        """
        config = {}
        self.load_check(config)
        self.check._is_excluded = MagicMock(return_value=False)

        content_mock = self.mock_content("host")
        obj_list = self.check._vsphere_hosts(content_mock, "ESXi")
        # Check if host has tags name and topo_type
        self.assertEqual(len(obj_list), 1)
        self.assertEqual(obj_list[0]['topo_tags']['name'], 'localhost.localdomain')
        self.assertEqual(obj_list[0]['topo_tags']['topo_type'], 'vsphere-HostSystem')
        # Check if host list contains vm, datastore and computeresource
        self.assertEqual(obj_list[0]['topo_tags']['vms'][0], 'Ubuntu')
        self.assertEqual(obj_list[0]['topo_tags']['datastores'][0], 'WDC1TB')
        self.assertEqual(obj_list[0]['topo_tags']['computeresource'], 'localhost')

    def test_vsphere_clustercomputeresources(self):
        """
        Test if the vsphere_clustercomputeresources returns the cluster list
        """
        config = {}
        self.load_check(config)
        self.check._is_excluded = MagicMock(return_value=False)

        content_mock = self.mock_content("cluster")
        obj_list = self.check._vsphere_clustercomputeresources(content_mock, "ESXi")
        # Check if clustercomputeresources has tags name and topo_type
        self.assertEqual(len(obj_list), 1)
        self.assertEqual(obj_list[0]['topo_tags']['name'], 'local')
        self.assertEqual(obj_list[0]['topo_tags']['topo_type'], 'vsphere-ClusterComputeResource')
        # Check if clustercomputeresources list contains host and datastore
        self.assertEqual(obj_list[0]['topo_tags']['hosts'][0], 'localhost.localdomain')
        self.assertEqual(obj_list[0]['topo_tags']['datastores'][0], 'WDC1TB')

    def test_vsphere_computeresources(self):
        """
        Test if the vsphere_computeresources returns the computeresource list
        """
        config = {}
        self.load_check(config)
        self.check._is_excluded = MagicMock(return_value=False)

        content_mock = self.mock_content("compute")
        obj_list = self.check._vsphere_computeresources(content_mock, "ESXi")
        # Check if computeresources has tags name and topo_type
        self.assertEqual(len(obj_list), 1)
        self.assertEqual(obj_list[0]['topo_tags']['name'], 'localhost')
        self.assertEqual(obj_list[0]['topo_tags']['topo_type'], 'vsphere-ComputeResource')
        # Check if computeresources list contains host and datastore
        self.assertEqual(obj_list[0]['topo_tags']['hosts'][0], 'localhost.localdomain')
        self.assertEqual(obj_list[0]['topo_tags']['datastores'][0], 'WDC1TB')

    def test_vsphere_vms_with_regex(self):
        """
        Test if the vsphere_vms_regex returns the empty VM list
        """
        config = {}
        self.load_check(config)

        content_mock = self.mock_content("vm")
        regex = {"vm_include": "host12"}
        obj_list_regex = self.check._vsphere_vms(content_mock, domain="ESXi", regexes=regex)

        self.assertEqual(len(obj_list_regex), 0)

    def test_get_topologyitems_sync(self):
        """
        Test if it returns the topology items and tags for VM
        """
        instance = {'name': 'vsphere_mock', 'host': "ESXi"}
        config = {}
        self.load_check(config)
        self.check._is_excluded = MagicMock(return_value=False)

        server_mock = MagicMock()
        server_mock.configure_mock(**{'RetrieveContent.return_value': self.mock_content("vm")})
        self.check._get_server_instance = MagicMock(return_value=server_mock)

        topo_dict = self.check.get_topologyitems_sync(instance)
        self.assertEqual(len(topo_dict["vms"]), 1)

        # Check if tags are as expected
        self.assertEqual(topo_dict["vms"][0]['topo_tags']['name'], 'Ubuntu')
        self.assertEqual(topo_dict["vms"][0]['topo_tags']['domain'], 'ESXi')
        self.assertEqual(topo_dict["vms"][0]['topo_tags']['layer'], 'VSphere VMs')
        self.assertEqual(topo_dict["vms"][0]["topo_tags"]["topo_type"], "vsphere-VirtualMachine")
        self.assertEqual(topo_dict["vms"][0]['topo_tags']['datastore'], '54183927-04f91918-a72a-6805ca147c55')

    def test_collect_topology_component(self):
        """
        Test the component collection from the topology for VirtualMachine
        """
        config = {}
        self.load_check(config)
        instance = {'name': 'vsphere_mock', 'host': 'test-esxi'}
        topo_items = {'datastores': [], 'clustercomputeresource': [], 'computeresource': [], 'hosts': [], 'datacenters':
            [], 'vms': [{'hostname': 'Ubuntu', 'topo_tags': {'topo_type': 'vsphere-VirtualMachine',
                 'name': 'Ubuntu', 'datastore': '54183927-04f91918-a72a-6805ca147c55'}, 'mor_type': 'vm'}]}
        self.check.get_topologyitems_sync = MagicMock(return_value=topo_items)
        self.check.collect_topology(instance)
        topo_instances = self.check.get_topology_instances()

        # Check if the returned topology contains 1 component
        self.assertEqual(len(topo_instances), 1)
        self.assertEqual(len(topo_instances[0]['components']), 1)
        self.assertEqual(topo_instances[0]['components'][0]['externalId'],
                         'urn:vsphere:/test-esxi/vsphere-VirtualMachine/Ubuntu')

    def test_collect_topology_comp_relations(self):
        """
        Test the collection of components and relations from the topology for Datastore
        """
        topo_items = {"datastores": [{'mor_type': 'datastore','topo_tags': {'accessible': True, 'topo_type':
            'vsphere-Datastore', 'capacity': 999922073600L, 'name': 'WDC1TB', 'url':
            '/vmfs/volumes/54183927-04f91918-a72a-6805ca147c55', 'type': 'VMFS', 'vms': ['UBUNTU_SECURE', 'W-NodeBox',
            'NAT', 'Z_CONTROL_MONITORING (.151)', 'LEXX (.40)', 'parrot']}}], "vms": [], 'clustercomputeresource': [],
            'computeresource': [], 'hosts': [], 'datacenters': []}

        config = {}
        self.load_check(config)
        instance = {'name': 'vsphere_mock', 'host': 'test-esxi'}
        self.check.get_topologyitems_sync = MagicMock(return_value=topo_items)
        self.check.collect_topology(instance)
        topo_instances = self.check.get_topology_instances()

        # Check if the returned topology contains 1 component
        self.assertEqual(len(topo_instances), 1)
        self.assertEqual(len(topo_instances[0]['components']), 1)
        self.assertEqual(topo_instances[0]['components'][0]['externalId'],
                         'urn:vsphere:/test-esxi/vsphere-Datastore/WDC1TB')

        # Check if the returned topology contains 6 relations for 6 VMs
        self.assertEqual(len(topo_instances[0]['relations']), 6)
        self.assertEqual(topo_instances[0]['relations'][0]['type']['name'], 'vsphere-vm-uses-datastore')

    def test_get_topologyitems_with_vm_regexes(self):
        """
        Test if it returns the vm as per filter config
        """
        instance = {'name': 'vsphere_mock', 'host': "ESXi", "vm_include_only_regex": "VM"}
        config = {}
        self.load_check(config)
        # self.check._is_excluded = MagicMock(return_value=False)

        server_mock = MagicMock()
        server_mock.configure_mock(**{'RetrieveContent.return_value': self.mock_content("vm")})
        self.check._get_server_instance = MagicMock(return_value=server_mock)

        topo_dict = self.check.get_topologyitems_sync(instance)
        self.assertEqual(len(topo_dict["vms"]), 0)

    def test_get_topologyitems_with_host_regexes(self):
        """
        Test if it returns the hosts as per filter config
        """
        instance = {'name': 'vsphere_mock', 'host': "ESXi", "host_include_only_regex": "localhost"}
        config = {}
        self.load_check(config)
        # self.check._is_excluded = MagicMock(return_value=False)

        server_mock = MagicMock()
        server_mock.configure_mock(**{'RetrieveContent.return_value': self.mock_content("host")})
        self.check._get_server_instance = MagicMock(return_value=server_mock)

        topo_dict = self.check.get_topologyitems_sync(instance)
        self.assertEqual(len(topo_dict["hosts"]), 1)
