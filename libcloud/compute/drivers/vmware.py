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
import collections
import ipaddress
import re
import ssl

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
from libcloud.utils import misc as misc_utils


__all__ = [
    'VSphereNodeDriver',
]


DEFAULT_CONNECTION_TIMEOUT = 5  # default connection timeout in seconds
DEFAULT_PAGE_SIZE = 5


class VMwarePropertyCollectorPaginator(misc_utils.PageList):
    """
    The paginator for the PropertyCollector.

    See:
     - http://pubs.vmware.com/vsphere-50/index.jsp#com.vmware.wssdk.pg.doc_50/PG_Ch5_PropertyCollector.7.2.html
    """

    def __init__(
            self, connection, object_cls, path_set,
            process_fn=None, page_size=DEFAULT_PAGE_SIZE):
        """
        :param connection: The connection to VSphere.
        :type connection: :class:`VSphereConnection`

        :param object_cls: Type of managed object.
        :type object_cls: :class:`vim.ManagedEntity`

        :param path_set: List of properties to retrieve.
        :type path_set: list

        :param page_size: [description], defaults to None.
        :param page_size: int, optional

        :param process_fn: [description], defaults to None.
        :param process_fn: callable, optional
        """
        self._connection = connection
        self._object_cls = object_cls
        self._path_set = path_set
        self._collector = None

        def _process_fn(result):
            """Convert result to dict before processing."""
            result = {
                prop.obj: {prop.name: prop.val for prop in prop.propSet}
                for prop in result.objects}
            return process_fn(result) if process_fn else result

        super(VMwarePropertyCollectorPaginator, self).__init__(
            request_fn=self._retrieve_properties,
            process_fn=_process_fn,
            page_size=page_size,
            request_args=[],
            request_kwargs={})

    def extract_next_page_token(self, response):
        """
        :type response: :class:`vmodl.query.PropertyCollector.RetrieveResult`
        """
        return response.token

    def _retrieve_properties(self):
        if self.next_page_token is None:
            self._collector, retrieve_args = self._create_property_collector()
            return self._collector.RetrievePropertiesEx(*retrieve_args)
        return self._collector.ContinueRetrievePropertiesEx(
            token=self.next_page_token)

    def _create_property_collector(self):
        """
        Initializes the property collector.

        :return: The property collector and paramaters for
            :meth:`vmodl.query.PropertyCollector.RetrieveProperties` method.
        :rtype: tulpe(:class:`vmodl.query.PropertyCollector`, tuple)
        """
        if not self._path_set:
            raise ValueError("Empty 'path_set' is specified")

        content = self._connection.client.RetrieveContent()
        collector = content.propertyCollector
        container_view = content.viewManager.CreateContainerView(
            container=content.rootFolder,
            type=[self._object_cls],
            recursive=True,
        )  # type: vim.view.ContainerView

        # Create a traversal specification to identify the path for
        # collection
        traversal_spec = vmodl.query.PropertyCollector.TraversalSpec(
            name='traverseEntities',
            path='view',
            skip=False,
            type=container_view.__class__)

        # Create an object specification to define the starting point
        # for inventory navigation
        object_spec = vmodl.query.PropertyCollector.ObjectSpec(
            obj=container_view,
            skip=True,
            selectSet=[traversal_spec])

        # Identify the properties to the retrieved
        property_spec = vmodl.query.PropertyCollector.PropertySpec(
            type=self._object_cls,
            pathSet=self._path_set)

        # Add the object and property specification to the property
        # filter specification
        filter_spec = vmodl.query.PropertyCollector.FilterSpec(
            objectSet=[object_spec],
            propSet=[property_spec])

        # Configure max page size
        options = vmodl.query.PropertyCollector.RetrieveOptions(
            maxObjects=self.page_size)

        return collector, ([filter_spec], options)


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
        super(VSphereConnection, self).__init__(
            user_id=user_id,
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
        super(VSphereNodeDriver, self).__init__(
            key=username, secret=password,
            secure=secure, host=host,
            port=port, url=url,
            timeout=timeout, **kwargs)

    def _ex_connection_class_kwargs(self):
        kwargs = {
            'url': self.url
        }

        return kwargs

    def list_nodes(self, ex_page_size=None):
        """
        Lists available nodes (excluding templates).
        """
        return list(self.iterate_nodes(
            ex_page_size=ex_page_size,
        ))

    def iterate_nodes(self, ex_page_size=None):
        """
        Iterate available nodes (excluding templates).
        """
        creation_times = self._query_node_creation_times()

        def result_to_nodes(result):
            for vm_entity, vm_properties in result.items():
                if vm_properties['summary.config'].template is True:
                    continue
                node = self._to_node(vm_entity, vm_properties)
                node.created_at = creation_times.get(node.id)
                yield node

        return VMwarePropertyCollectorPaginator(
            self.connection,
            object_cls=vim.VirtualMachine,
            path_set=[
                'summary',
                'summary.config',
                'summary.runtime',
                'summary.guest',
            ],
            process_fn=result_to_nodes,
            page_size=ex_page_size,
        )

    def list_images(self, ex_page_size=None):
        """
        List available images (templates).
        """
        return list(self.iterate_images(
            ex_page_size=ex_page_size,
        ))

    def iterate_images(self, ex_page_size=None):
        """
        Iterate available images (templates).
        """
        creation_times = self._query_node_creation_times()

        def result_to_images(result):
            for vm_entity, vm_properties in result.items():
                if vm_properties['summary.config'].template is False:
                    continue
                image = self._to_image(vm_entity, vm_properties)
                image.created_at = creation_times.get(image.id)
                yield image

        return VMwarePropertyCollectorPaginator(
            self.connection,
            object_cls=vim.VirtualMachine,
            path_set=[
                'summary.config',
            ],
            process_fn=result_to_images,
            page_size=ex_page_size,
        )

    def list_volumes(self, node=None):
        """
        List available volumes (on all datastores).
        """
        if node is None:
            virtual_machines = self._list_virtual_machines()
        else:
            virtual_machines = [self.ex_get_vm(node)]

        # grouping vm's disks by unique backing-file name
        grouped_vm_disks = collections.defaultdict(list)
        for vm in virtual_machines:
            for vm_disk in self._get_vm_virtual_disks_properties(vm):
                backing_file = vm_disk['device']['file_name']
                grouped_vm_disks[backing_file].append(vm_disk)

        datastores_info = [
            datastore.info for datastore in self._list_datastores()
        ]  # type: list[vim.host.VmfsDatastoreInfo]
        volumes = [
            self._to_volume(vm_disks, datastores_info)
            for vm_disks in grouped_vm_disks.values()]

        # update volume creation timestamps
        vm_entity = virtual_machines[0] if len(virtual_machines) == 1 else None
        creation_times = self._query_volume_creation_times(
            volumes, virtual_machine=vm_entity)
        for volume in volumes:
            volume_file = volume.extra['file_path']
            volume.extra['created_at'] = creation_times.get(volume_file)

        return volumes

    def iterate_volumes(self, ex_page_size=None):
        raise NotImplementedError()

    def list_snapshots(self, ex_page_size=None):
        """
        List available snapshots.
        """
        return list(self.iterate_snapshots(
            ex_page_size=ex_page_size,
        ))

    def iterate_snapshots(self, ex_page_size=None):
        """
        Iterate available snapshots.
        """
        def result_to_snapshots(result):
            for vm_properties in result.values():
                snapshot_trees = vm_properties.get('snapshot.rootSnapshotList')
                for snapshot_tree in self._walk_snapshot_tree(snapshot_trees):
                    yield self._to_snapshot(snapshot_tree, vm_properties)

        return VMwarePropertyCollectorPaginator(
            self.connection,
            object_cls=vim.VirtualMachine,
            path_set=[
                'snapshot.rootSnapshotList',
                'layoutEx.snapshot',
                'layoutEx.file',
            ],
            process_fn=result_to_snapshots,
            page_size=ex_page_size,
        )

    def _walk_snapshot_tree(self, snapshot_tree):
        """
        :param snapshot_tree: vim.vm.SnapshotTree

        :rtype: list[:class:`vim.vm.SnapshotTree`]
        """
        snapshot_data = []
        for item in snapshot_tree or []:
            snapshot_data.append(item)
            child_tree = item.childSnapshotList
            snapshot_data.extend(self._walk_snapshot_tree(child_tree))
        return snapshot_data

    def _query_node_creation_times(self, virtual_machine=None):
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

    def _query_volume_creation_times(self, volumes, virtual_machine=None):
        """
        Fetches the creation dates of the volumes from the event history.

        :type volumes: list[:class:`Volumes`]
        :type virtual_machine: :class:`vim.VirtualMachine`
        :rtype: dict[str, :class:`datetime.datetime`]
        """
        reconfigure_events = self._query_events(
            event_type_id='VmReconfiguredEvent',
            entity=virtual_machine,
        )  # type: list[vim.Event]

        volume_creation_times = {}  # type: dict[str, datetime.datetime]
        node_creation_times = self._query_node_creation_times(
            virtual_machine=virtual_machine,
        )  # type: list[vim.Event]

        # 1. Root volumes
        #
        # The default SCSI controller is numbered as 0. When you create a
        # virtual machine, the default hard disk is assigned to the default
        # SCSI controller 0 at bus node (0:0).
        # By default, the SCSI controller is assigned to virtual device
        # node (z:7).
        for volume in volumes:
            volume_file = volume.extra['file_path']
            for node_id, node_devices in volume.extra['devices'].items():
                for device in node_devices:
                    if device['scsi_unit_number'] == 7 \
                            and device['unit_number'] == 0 \
                            and device['scsi_bus_number'] == 0:
                        volume_creation_times[volume_file] = node_creation_times.get(node_id)
                        continue

        # 2. Extra volumes
        for event in reconfigure_events:  # lazy iterator with API pagination
            created_files = (
                change.device.backing.fileName
                for change in event.configSpec.deviceChange
                if isinstance(change.device, vim.vm.device.VirtualDisk) \
                    and change.operation == 'add' \
                    and change.fileOperation == 'create')

            for created_file in created_files:
                volume_creation_times.update({
                    volume.extra['file_path']: event.createdTime
                    for volume in volumes
                    if volume.extra['file_path'] == created_file})

        return volume_creation_times

    def _query_vm_virtual_disks(self, virtual_machines=None):
        """
        Return properties of :class:`vim.vm.device.VirtualDisk`.
        See:
         - https://pubs.vmware.com/vi3/sdk/ReferenceGuide/vim.vm.device.VirtualDisk.html
        :param virtual_machine: The virtual machine.
        :type virtual_machine: :class:`vim.VirtualMachine`
        :rtype: list[:class:`_VMDiskInfo`]
        """
        # TODO: use property collector
        virtual_machines = virtual_machines or self._list_virtual_machines()
        datastores_info = [ds.info for ds in self._list_datastores()]

        result = []
        for virtual_machine in virtual_machines:
            # pylint: disable=protected-access
            vm_id = virtual_machine._GetMoId()
            vm_config = virtual_machine.config
            if not vm_config:
                continue

            vm_devices = vm_config.hardware.device
            vm_disks = {
                entity for entity in vm_devices
                if isinstance(entity, vim.vm.device.VirtualDisk)}
            scsi_controller_to_device_map = {
                device: {
                    'unit_number': entity.scsiCtlrUnitNumber,
                    'bus_number': entity.busNumber}
                for entity in vm_devices
                if isinstance(entity, vim.vm.device.VirtualSCSIController)
                for device in entity.device}

            for disk in vm_disks:
                backing = disk.backing
                device_info = disk.deviceInfo
                if not isinstance(backing, vim.vm.device.VirtualDevice.FileBackingInfo):
                    continue

                file_path = self._file_name_to_path(
                    backing.fileName,
                    datastores_info=datastores_info)
                disk_info = _VMDiskInfo(
                    disk_id=disk.diskObjectId,
                    owner_id=vm_id,
                    file_path=file_path,
                    size=disk.capacityInKB,
                    label=device_info.label if device_info else None,
                    summary=device_info.summary if device_info else None,
                    sharing=backing.sharing == 'sharingMultiWriter',
                    disk_mode=backing.diskMode)
                if disk.key in scsi_controller_to_device_map:
                    scsi_controller = scsi_controller_to_device_map[disk.key]
                    disk_info.scsi_host = scsi_controller['unit_number']
                    disk_info.scsi_bus = scsi_controller['bus_number']
                    disk_info.scsi_target = disk.controllerKey
                    disk_info.scsi_lun = disk.unitNumber

                result.append(disk_info)
        return result

    def ex_get_vm(self, node_or_uuid):
        """
        Searches VMs for a given instance_uuid or Node object.

        :type node_or_uuid: :class:`Node` | str
        :rtype: :class:`vim.VirtualMachine`
        """
        if isinstance(node_or_uuid, Node):
            node_or_uuid = node_or_uuid.extra['instance_uuid']
        content = self._retrieve_content()
        vm = content.searchIndex.FindByUuid(
            None, node_or_uuid, True, True)
        if not vm:
            raise LibcloudError("Unable to locate VirtualMachine.")
        return vm

    def _walk_folder(self, folder):
        """
        Recursively walks the specified folder and return a flat list of
        all files found.

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
        content = self._retrieve_content()
        return [
            entity for entity in content.rootFolder.childEntity
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

    def _retrieve_content(self):
        """
        Retrieves the properties of the service instance.

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

        content = self._retrieve_content()
        history_collector = content.eventManager.CreateCollectorForEvents(
            filter_spec)  # type: vim.event.EventHistoryCollector
        history_collector.SetCollectorPageSize(DEFAULT_PAGE_SIZE)
        history_collector.ResetCollector()

        events = history_collector.latestPage
        while events:
            for event in events:
                yield event
            events = history_collector.ReadPreviousEvents(DEFAULT_PAGE_SIZE)

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
            backing = device.backing
            if isinstance(backing, vim.vm.device.VirtualDevice.FileBackingInfo):
                virtual_disk['disk_mode'] = backing.diskMode
                virtual_disk['sharing'] = backing.sharing == 'sharingMultiWriter'
                virtual_disk['file_name'] = backing.fileName
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
                    if file_key not in files:
                        continue
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
        return self._to_node(vm_entity=self.ex_get_vm(uuid))

    def _to_node(self, vm_entity, vm_properties=None):
        """
        Creates :class:`Node` object from :class:`vim.VirtualMachine`.
        """
        if vm_properties is None:
            vm_properties = {}
        summary = vm_properties.get('summary') or vm_entity.summary
        config = vm_properties.get('summary.config') or vm_entity.config
        runtime = vm_properties.get('summary.runtime') or vm_entity.runtime
        guest = vm_properties.get('summary.guest') or vm_entity.guest

        extra = {
            'uuid': config.uuid,
            'instance_uuid': config.instanceUuid,
            'path': config.vmPathName,
            'guest_id': config.guestId,
            'template': config.template,
            'overall_status': str(summary.overallStatus),
            'operating_system': config.guestFullName,
            'cpus': config.numCpu,
            'memory_mb': config.memorySizeMB,
            'boot_time': None,
            'annotation': None,
        }

        boot_time = runtime.bootTime
        if boot_time:
            extra['boot_time'] = boot_time.isoformat()

        annotation = config.annotation
        if annotation:
            extra['annotation'] = annotation

        public_ips = []
        private_ips = []
        if guest is not None and guest.ipAddress is not None:
            ip_addr = ipaddress.ip_address(u'{}'.format(guest.ipAddress))
            if isinstance(ip_addr, ipaddress.IPv4Address):
                ip_addr = str(ip_addr)
                if is_public_subnet(ip_addr):
                    public_ips.append(ip_addr)
                else:
                    private_ips.append(ip_addr)

        state = self.NODE_STATE_MAP.get(
            runtime.powerState,
            NodeState.UNKNOWN)

        node = Node(
            id=vm_entity._GetMoId(),
            name=summary.config.name,
            state=state,
            public_ips=public_ips,
            private_ips=private_ips,
            driver=self,
            extra=extra)
        return node

    def _to_image(self, vm_entity, vm_properties=None):
        """
        Creates :class:`NodeImage` object from :class:`vim.VirtualMachine`.
        """
        if vm_properties is None:
            vm_properties = {}
        config = vm_properties.get('summary.config') or vm_entity.summary.config

        return NodeImage(
            id=config.uuid,
            name=config.name,
            extra={
                # pylint: disable=protected-access
                'managed_object_id': vm_entity._GetMoId(),
            },
            driver=self)

    def _to_volume(self, disks, datastores_info):
        """
        Creates :class:`StorageVolume` object from disk properties.

        :param dict disk_properties: dict in format that
            :meth:`self._get_vm_virtual_disks_properties` returns.

        :param disks: dict(s) in format that :meth:`self._get_vm_disks` returns.
        :type disks: dict | list[dict]
        """
        if isinstance(disks, dict):
            disks = [disks]
        elif len(disks) > 1 and not all(disk['device']['sharing'] is True for disk in disks):
            disks_ids = [disk['device']['disk_object_id'] for disk in disks]
            raise LibcloudError((
                "Unable to create StorageVolume from multiple non-shared "
                "disks: {}."
            ).format(', '.join(disks_ids)))

        main_disk = disks[0]
        volume_id = main_disk['device']['key']
        name = main_disk['label']
        size = int(main_disk['capacity'])

        extra = {
            key: value for key, value in main_disk.items()
            if key != 'device'}
        extra['file_path'] = self._file_name_to_path(
            main_disk['device']['file_name'],
            datastores_info=datastores_info)
        extra['devices'] = collections.defaultdict(list)
        for disk in disks:
            extra['devices'][disk['owner_id']].append(disk['device'])

        return StorageVolume(
            id=volume_id,
            name=name,
            size=size,
            driver=self,
            extra=extra)

    def _to_snapshot(self, snapshot_tree, vm_properties=None):
        """
        Creates :class:`VolumeSnapshot` object from :class:`vim.vm.SnapshotTree`.

        See:
          - https://pubs.vmware.com/vi3/sdk/ReferenceGuide/vim.vm.SnapshotTree.html
          - https://pubs.vmware.com/vi3/sdk/ReferenceGuide/vim.vm.Snapshot.html

        :type snapshot_tree: :class:`vim.vm.SnapshotTree`
        :type snapshot_layout: :class:`vim.vm.FileLayoutEx.SnapshotLayout`
        :type file_layout: :class:`vim.vm.FileLayoutEx.FileInfo`

        :returns: Storage volume snapthot.
        :rtype: :class:`VolumeSnapshot`
        """
        if vm_properties is None:
            vm_properties = {}

        vm_entity = snapshot_tree.vm
        snapshot_layout = vm_properties.get('layoutEx.snapshot')
        file_layout = vm_properties.get('layoutEx.file')
        if snapshot_layout is None or file_layout is None:
            layout_ex = vm_entity.layoutEx
            snapshot_layout = snapshot_layout or layout_ex.snapshot
            file_layout = file_layout or layout_ex.file

        extra = {
            'name': snapshot_tree.name,
            'description': snapshot_tree.description,
            'created_at': snapshot_tree.createTime,
            'quiesced': snapshot_tree.quiesced,
            'backup_manifest': snapshot_tree.backupManifest,
            'replay_supported': snapshot_tree.replaySupported,
            'owner_state': snapshot_tree.state,
            # pylint: disable=protected-access
            'owner_id': snapshot_tree.vm._GetMoId()
        }

        if snapshot_layout is None or file_layout is None:
            capacity_in_kb = None
        else:
            snapshot = snapshot_tree.snapshot
            snapshot_data_keys = [
                item.dataKey for item in snapshot_layout
                if item.key == snapshot]
            capacity_in_kb = sum(
                disk.size for disk in file_layout
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
