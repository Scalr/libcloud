# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
VMware vSphere driver using pyvmomi - https://github.com/vmware/pyvmomi
"""

import atexit
import ipaddress
import re
import ssl

from libcloud.common.base import ConnectionUserAndKey
from libcloud.common.types import InvalidCredsError
from libcloud.common.types import LibcloudError
from libcloud.compute.base import Node
from libcloud.compute.base import NodeDriver
from libcloud.compute.base import NodeImage
from libcloud.compute.base import StorageVolume
from libcloud.compute.base import VolumeSnapshot
from libcloud.compute.types import NodeState
from libcloud.compute.types import Provider
from libcloud.utils.networking import is_public_subnet

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

try:
    from pyVim import connect
    from pyVmomi import vim
    from pyVmomi import vmodl
except ImportError:
    raise ImportError('Missing "pyvmomi" dependency. You can install it '
                      'using pip - pip install pyvmomi')



__all__ = [
    'VSphereNodeDriver',
]


DEFAULT_CONNECTION_TIMEOUT = 5  # default connection timeout in seconds


class VSphereConnection(ConnectionUserAndKey):
    def __init__(self, user_id, key, secure=True,
                 host=None, port=None, url=None, timeout=None, **kwargs):
        if host and url:
            raise ValueError('host and url arguments are mutually exclusive.')
        if not host and not url:
            raise ValueError('Either host or url argument is required.')

        if url:
            host = urlparse.urlparse(url).netloc
        if not host:
            raise ValueError('Either "host" or "url" argument must be '
                             'provided')
        self.client = None
        super(VSphereConnection, self).__init__(user_id=user_id,
                                                key=key, secure=secure,
                                                host=host, port=port,
                                                timeout=timeout, **kwargs)

    def connect(self, **kwargs):
        kwargs.pop('secure', None)
        kwargs.pop('timeout', None)
        kwargs.pop('proxy_url', None)
        kwargs.pop('retry_delay', None)
        kwargs.pop('backoff', None)
        if 'host' not in kwargs:
            kwargs['host'] = self.host
        if 'user' not in kwargs:
            kwargs['user'] = self.user_id
        if 'pwd' not in kwargs:
            kwargs['pwd'] = self.key
        if 'sslContext' not in kwargs:
            kwargs['sslContext'] = ssl._create_unverified_context()

        try:
            self.client = connect.SmartConnect(**kwargs)
        except Exception as e:
            message = '{}'.format(e)
            if 'incorrect user name' in message:
                raise InvalidCredsError('Check that your username and '
                                        'password are valid.')
            if 'connection refused' in message \
                    or 'is not a vim server' in message:
                raise LibcloudError('Check that the host provided is a '
                                    'vSphere installation.')
            if 'name or service not known' in message:
                raise LibcloudError('Check that the vSphere host is accessible.')

        atexit.register(connect.Disconnect, self.client)


class VSphereNodeDriver(NodeDriver):
    name = 'VMware vSphere'
    website = 'http://www.vmware.com/products/vsphere/'
    type = Provider.VSPHERE
    connectionCls = VSphereConnection

    NODE_STATE_MAP = {
        'poweredOn': NodeState.RUNNING,
        'poweredOff': NodeState.STOPPED,
        'suspended': NodeState.SUSPENDED,
    }

    def __init__(self, username, password, secure=True,
                 host=None, port=None, url=None, timeout=None, **kwargs):
        self.url = url
        super(VSphereNodeDriver, self).__init__(key=username, secret=password,
                                                secure=secure, host=host,
                                                port=port, url=url,
                                                timeout=timeout, **kwargs)

    def _ex_connection_class_kwargs(self):
        kwargs = {
            'url': self.url
        }

        return kwargs

    def list_nodes(
            self,
            ex_datacenter=None,
            ex_cluster=None,
            ex_resource_pool=None):
        """
        Lists available nodes (excluding templates).
        """
        if ex_cluster is not None:
            raise NotImplementedError(
                'ex_cluster filter is not implemented yet.')
        if ex_resource_pool is not None:
            raise NotImplementedError(
                'ex_resource_pool filter is not implemented yet.')

        virtual_machines = self._list_virtual_machines(
            datacenter=ex_datacenter,
            cluster=ex_cluster,
            resource_pool=ex_resource_pool,
            is_template=False)
        nodes = [self._to_node(vm) for vm in virtual_machines]

        creation_dates = self._query_nodes_creation_dates()
        for node in nodes:
            node.extra['create_time'] = creation_dates.get(node.id)

        return nodes

    def list_images(self, ex_datacenter=None):
        """
        List available images (templates).
        """
        virtual_machines = self._list_virtual_machines(
            datacenter=ex_datacenter,
            is_template=True)
        images = [self._to_image(vm_image) for vm_image in virtual_machines]

        creation_dates = self._query_nodes_creation_dates()
        for image in images:
            image.extra['create_time'] = creation_dates.get(image.id)

        return images

    def list_volumes(self, node=None):
        """
        List available volumes (on all datastores).
        """
        if node is not None:
            virtual_machines = [self.ex_get_vm(node)]
        else:
            virtual_machines = self._list_virtual_machines()

        volumes = []
        for vm in virtual_machines:
            volumes.extend([
                self._to_volume(prop)
                for prop in self._get_vm_virtual_disks_properties(vm)])

        filter_by_vm = virtual_machines[0] if node is not None else None
        creation_dates = self._query_volumes_creation_dates(
            volumes, virtual_machine=filter_by_vm)
        for volume in volumes:
            device_id = volume.extra['device']['disk_object_id']
            volume.extra['create_time'] = creation_dates.get(device_id)

        return volumes

    def list_snapshots(self, node=None):
        """
        List available snapshots.
        """
        snapshots = []
        virtual_machines = [
            self.ex_get_vm(node)
        ] if node is not None else self._list_virtual_machines()

        for vm in virtual_machines:
            snapshot_info = vm.snapshot
            if not isinstance(snapshot_info, vim.vm.SnapshotInfo):
                continue
            snapshots.extend(
                self._walk_snapshot_tree(snapshot_info.rootSnapshotList))
        return [self._to_snapshot(item) for item in snapshots]

    def _walk_snapshot_tree(self, snapshot_tree):
        """
        :param snapshot_tree: vim.vm.SnapshotTree

        :rtype: list[:class:`vim.vm.SnapshotTree`]
        """
        snapshot_data = []
        for item in snapshot_tree:
            snapshot_data.append(item)
            child_tree = item.childSnapshotList
            if child_tree:
                snapshot_data.extend(self._walk_snapshot_tree(child_tree))
        return snapshot_data

    def _query_nodes_creation_dates(self, virtual_machine=None):
        """
        Fetches the creation dates of the VMs from the event history.

        :type virtual_machine: :class:`vim.VirtualMachine`
        :rtype: dict[str, :class:`datetime.datetime`]
        """
        created_events = self._query_events(
            event_type_id=[
                'VmBeingDeployedEvent',
                'VmCreatedEvent',
                'VmRegisteredEvent',
                'VmClonedEvent'],
            entity=virtual_machine)  # type: list[vim.Event]
        return {
            # pylint: disable=protected-access
            event.vm.vm._GetMoId(): event.createdTime
            for event in created_events}

    def _query_volumes_creation_dates(self, volumes, virtual_machine=None):
        """
        Fetches the creation dates of the volumes from the event history.

        :type volumes: list[:class:`Volumes`]
        :type virtual_machine: :class:`vim.VirtualMachine`
        :rtype: dict[str, :class:`datetime.datetime`]
        """
        reconfigure_events = self._query_events(
            event_type_id='VmReconfiguredEvent',
            entity=virtual_machine,
            system_user=True,
        )  # type: list[vim.Event]

        nodes_creation_dates = self._query_nodes_creation_dates(
            virtual_machine=virtual_machine,
        )  # type: list[vim.Event]

        datastores_info = [
            datastore.info for datastore in self._list_datastores()
        ]  # type: list[vim.host.VmfsDatastoreInfo]
        creation_dates = {}  # type: dict[str, datetime.datetime]
        extra_volumes_files = {} # type: dict[str, str]

        for volume in volumes:
            device = volume.extra['device']
            device_id = device['disk_object_id']

            # 1. Root volume
            #
            # The default SCSI controller is numbered as 0. When you create a
            # virtual machine, the default hard disk is assigned to the default
            # SCSI controller 0 at bus node (0:0).
            # By default, the SCSI controller is assigned to virtual device
            # node (z:7).
            if device['scsi_unit_number'] == 7 and device['scsi_bus_number'] == 0:
                cloud_instance_id = volume.extra['owner_id']
                creation_dates[device_id] = nodes_creation_dates.get(cloud_instance_id)
                continue

            # 2. Extra volume
            for volume_file in volume.extra['files']:
                volume_file_path = self._file_name_to_path(
                    volume_file['name'],
                    datastores_info=datastores_info)
                extra_volumes_files[volume_file_path] = device_id

        # Search for timestamps of the extra volumes (2)
        for event in reconfigure_events:  # lazy iterator with API pagination
            reconfigured_devices = [
                change.device for change in event.configSpec.deviceChange
                if isinstance(change.device, vim.vm.device.VirtualDisk) \
                    and change.operation == 'add' \
                    and change.fileOperation == 'create']

            for device in reconfigured_devices:
                attached_file = device.backing.fileName
                if attached_file in extra_volumes_files:
                    device_id = extra_volumes_files[attached_file]
                    creation_dates[device_id] = event.createdTime
            if len(creation_dates) == len(volumes):
                break

        return creation_dates

    def ex_get_vm(self, node_or_uuid):
        """
        Searches VMs for a given instance_uuid or Node object.

        :type node_or_uuid: :class:`Node` | str
        :rtype: :class:`vim.VirtualMachine`
        """
        if isinstance(node_or_uuid, Node):
            node_or_uuid = node_or_uuid.extra['instance_uuid']
        vm = self._content.searchIndex.FindByUuid(
            None, node_or_uuid, True, True)
        if not vm:
            raise LibcloudError("Unable to locate VirtualMachine.")
        return vm

    def _list_virtual_machines(
            self,
            datacenter=None,
            cluster=None,
            resource_pool=None,
            is_template=None):
        """
        Lists available virtual machines and/or templates.

        :param is_template: bool
        """
        virtual_machines = []
        for child in self._content.rootFolder.childEntity:
            if not isinstance(child, vim.Datacenter):
                continue
            if datacenter is not None and child.name != datacenter:
                continue

            for vm in self._walk_folder(child.vmFolder):
                if is_template is not None \
                        and vm.summary.config.template != is_template:
                    continue
                virtual_machines.append(vm)
        return virtual_machines

    def _walk_folder(self, folder):
        """
        Recursively walks the specified folder.

        :type folder: :class:`vim.Folder`
        :rtype: list
        """
        result = []
        for item in folder.childEntity:
            if isinstance(item, vim.Folder):
                result.extend(self._walk_folder(item))
            else:
                result.append(item)
        return result

    def _list_datacenters(self):
        """
        Returns list of datacenters.

        See: https://pubs.vmware.com/vi3/sdk/ReferenceGuide/vim.Datacenter.html

        :rtype: list[:class:`vim.Datacenter`]
        """
        return [
            entity for entity in self._content.rootFolder.childEntity
            if isinstance(entity, vim.Datacenter)]

    def _list_datastores(self):
        """
        Returns the list of datastores.

        See: https://pubs.vmware.com/vi3/sdk/ReferenceGuide/vim.Datastore.html

        :rtype: list[:class:`vim.Datastore`]
        """
        return [
            entity for datacenter in self._list_datacenters()
            for entity in datacenter.datastore
            if isinstance(entity, vim.Datastore)]

    @property
    def _content(self):
        """
        Retrieves service content. 

        See: https://pubs.vmware.com/vi3/sdk/ReferenceGuide/vim.ServiceInstanceContent.html

        :rtype: :class:`vim.ServiceInstanceContent`
        """
        return self.connection.client.RetrieveContent()

    def _file_name_to_path(self, name, datastores_info=None):
        """
        Converts file name to full path.

        :type name: str
        :type datastores: list[:class:`vim.Datastore`]
        """
        match = re.match(r'^\[(.*?)\] ((\w|\W)*)$', name)
        if not match:
            raise LibcloudError("Unecpected file name format: {}".format(name))
        datastore_name, file_path = match.group(1, 2)

        if datastores_info is None:
            datastores_info = [ds.info for ds in self._list_datastores()]

        for datastore_info in datastores_info:
            if datastore_info.name == datastore_name:
                return '{}{}'.format(datastore_info.url, file_path)

        raise LibcloudError((
            "VMWare datastore '{}' not found."
        ).format(datastore_name))

    def _query_events(
            self,
            event_type_id=None,
            entity=None,
            begin_time=None,
            end_time=None,
            userlist=None,
            system_user=None):
        """
        Returns the events in specified filter. Returns empty array when
        there are not any events qualified.

        Note: vCenter Server stores events in the database for a limited period.
            The default number of days to retain event messages in the database
            is 30.

        :param event_type_id: This property, if set, limits the set of collected
            events to those specified types. (optional)
        :type event_type_id: str | list[str]

        :param entity: The filter specification for retrieving events by managed
            entity. If the property is not set, then events attached to all
            managed entities are collected. (optional)
        :type entity: :class:`vim.ManagedEntity`

        :param begin_time: The beginning of the time range. If this property is
            not set, then events are collected from the earliest
            time in the database. (optional)
        :type begin_time: :class:`datatime.datatime`

        :param end_time: The end of the time range. If this property is not
            specified, then events are collected up to the latest
            time in the database. (optional)
        :type end_time: :class:`datatime.datatime`

        :param userlist: This option specifies users used to filter event
            history. (optional)
        :type userlist: str | list[str]

        :param system_user: Filter by system user true for system user
            event. (optional)
        :type system_user: bool

        :rtype: list[:class:`vim.Event`]
        """
        filter_spec = vim.event.EventFilterSpec(eventTypeId=event_type_id)
        if entity is not None:
            filter_spec.entity = vim.event.EventFilterSpec.ByEntity(
                entity=entity,
                recursion='self')
        if begin_time is not None or end_time is not None:
            filter_spec.time = vim.event.EventFilterSpec.ByTime(
                beginTime=begin_time,
                endTime=end_time)
        if userlist is not None or system_user is not None:
            filter_spec.userName = vim.event.EventFilterSpec.ByUsername(
                userList=userlist,
                systemUser=system_user or False)

        history_collector = self._content.eventManager.CreateCollectorForEvents(
            filter_spec)  # type: vim.event.EventHistoryCollector
        history_collector.SetCollectorPageSize(1000)
        history_collector.ResetCollector()

        events = history_collector.latestPage
        while events:
            for event in events:
                yield event
            events = history_collector.ReadPreviousEvents(1000)

    def _get_vm_virtual_disks_properties(self, virtual_machine):
        """
        Return combined properties of :class:`vim.vm.device.VirtualDisk` and
            :class:`vim.vm.FileLayoutEx` objects.

        See:
         - https://pubs.vmware.com/vi3/sdk/ReferenceGuide/vim.vm.device.VirtualDisk.html
         - https://pubs.vmware.com/vsphere-6-5/topic/com.vmware.wssdk.apiref.doc/vim.vm.FileLayoutEx.html

        :param virtual_machine: The virtual machine.
        :type virtual_machine: :class:`vim.VirtualMachine`

        :rtype: list[dict]
        """
        layout_ex = virtual_machine.layoutEx
        if not layout_ex:
            return []

        cloud_instance_id = virtual_machine._GetMoId()
        vm_devices = virtual_machine.config.hardware.device
        vm_virtual_disks = {
            entity for entity in vm_devices
            if isinstance(entity, vim.vm.device.VirtualDisk)}
        vm_virtual_scsi_controllers = {
            entity for entity in vm_devices
            if isinstance(entity, vim.vm.device.VirtualSCSIController)}

        devices = {}
        for device in vm_virtual_disks:
            virtual_disk = {
                'key': device.key,
                'disk_object_id': device.diskObjectId,
                'capacity_in_kb': device.capacityInKB,
                'controller_key': device.controllerKey,  # optional
                'unit_number': device.unitNumber,  # optional
                'scsi_unit_number': None,
                'scsi_bus_number': None,
                'label': None,
                'summary': None}
            if device.deviceInfo:
                virtual_disk['label'] = device.deviceInfo.label
                virtual_disk['summary'] = device.deviceInfo.summary
            devices[device.key] = virtual_disk
        for scsi_controller in vm_virtual_scsi_controllers:
            for device_key in scsi_controller.device:
                devices[device_key].update({
                    'scsi_unit_number': scsi_controller.scsiCtlrUnitNumber,
                    'scsi_bus_number': scsi_controller.busNumber})

        files = {}
        for file_info in layout_ex.file:
            files[file_info.key] = {
                'accessible': file_info.accessible,
                'backing_object_id': file_info.backingObjectId,
                'name': file_info.name,
                'key': file_info.key,
                'size': file_info.size,
                'type': file_info.type,
                'unique_size': file_info.uniqueSize}

        properties = []
        for disk_info in layout_ex.disk:
            disk_files = []
            committed = 0
            descriptor = None
            for chain in getattr(disk_info, 'chain', ()):
                for file_key in chain.fileKey:
                    f = files[file_key]
                    disk_files.append(f)
                    if f['type'] == 'diskExtent':
                        committed += f['size']
                    if f['type'] == 'diskDescriptor':
                        descriptor = f['name']
            device = devices[disk_info.key]
            properties.append({
                'device': device,
                'files': disk_files,
                'capacity': device['capacity_in_kb'],
                'committed': int(committed / 1024),
                'descriptor': descriptor,
                'label': device['label'],
                'owner_id': cloud_instance_id
            })

        return properties

    def ex_get_node_by_uuid(self, uuid):
        """
        Searches Node for a given ``uuid``.

        :rtype: :class:`Node`
        """
        return self._to_node(self.ex_get_vm(uuid))

    def _to_node(self, virtual_machine):
        summary = virtual_machine.summary
        extra = {
            'uuid': summary.config.uuid,
            'instance_uuid': summary.config.instanceUuid,
            'path': summary.config.vmPathName,
            'guest_id': summary.config.guestId,
            'template': summary.config.template,

            'overall_status': str(summary.overallStatus),
            'operating_system': summary.config.guestFullName,

            'cpus': summary.config.numCpu,
            'memory_mb': summary.config.memorySizeMB
        }

        boot_time = summary.runtime.bootTime
        if boot_time:
            extra['boot_time'] = boot_time.isoformat()

        annotation = summary.config.annotation
        if annotation:
            extra['annotation'] = annotation

        public_ips = []
        private_ips = []
        if summary.guest is not None and summary.guest.ipAddress is not None:
            ip_addr = ipaddress.ip_address(
                u'{}'.format(summary.guest.ipAddress))
            if isinstance(ip_addr, ipaddress.IPv4Address):
                ip_addr = str(ip_addr)
                if is_public_subnet(ip_addr):
                    public_ips.append(ip_addr)
                else:
                    private_ips.append(ip_addr)

        state = self.NODE_STATE_MAP.get(summary.runtime.powerState,
                                        NodeState.UNKNOWN)

        node = Node(
            id=virtual_machine._GetMoId(),
            name=summary.config.name,
            state=state,
            public_ips=public_ips,
            private_ips=private_ips,
            driver=self,
            extra=extra)
        return node

    def _to_volume(self, disk_properties):
        """
        Creates :class:`StorageVolume` object from disk properties.

        :param dict disk_properties: dict in format that
            :meth:`self._get_vm_virtual_disks_properties` returns.

        :returns StorageVolume: Storage volume object
        """
        return StorageVolume(
            # XXX: use disk_properties['device']['disk_object_id'] instead
            id=disk_properties['device']['key'],
            name=disk_properties['label'],
            size=int(disk_properties['capacity']),
            driver=self,
            extra=disk_properties)

    def _to_snapshot(self, snapshot_tree):
        """
        Creates :class:`VolumeSnapshot` object from disk snapshot tree.

        See:
          - https://pubs.vmware.com/vi3/sdk/ReferenceGuide/vim.vm.SnapshotTree.html
          - https://pubs.vmware.com/vi3/sdk/ReferenceGuide/vim.vm.Snapshot.html

        :type snapshot: :class:`vim.vm.SnapshotTree`

        :returns: Storage volume snapthot.
        :rtype: :class:`VolumeSnapshot`
        """
        layout = snapshot_tree.vm.layoutEx
        snapshot = snapshot_tree.snapshot
        extra = {
            'name': snapshot_tree.name,
            'description': snapshot_tree.description,
            'create_time': snapshot_tree.createTime,
            'quiesced': snapshot_tree.quiesced,
            'backup_manifest': snapshot_tree.backupManifest,
            'replay_supported': snapshot_tree.replaySupported,
            'state': snapshot_tree.state,  # the power state of the virtual machine
        }
        snapshot_data_keys = [
            item.dataKey for item in layout.snapshot
            if item.key == snapshot]
        capacity_in_kb = sum(
            disk.size for disk in layout.file
            if disk.key in snapshot_data_keys and disk.type == 'snapshotData'
        ) / 1024.0

        return VolumeSnapshot(
            id=snapshot_tree.id,
            name=snapshot_tree.name,
            driver=self,
            size=int(capacity_in_kb),
            extra=extra,
            created=snapshot_tree.createTime,
            state=None)

    def _to_image(self, virtual_machine):
        config = virtual_machine.summary.config
        return NodeImage(
            id=config.uuid,
            name=config.name,
            driver=self)
