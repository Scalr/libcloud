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
import os

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

try:
    from pyVim import connect
    from pyVim import task as vmware_task
    from pyVmomi import vmodl
    from pyVmomi import vim
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


__all__ = [
    'VSphereNodeDriver',
]


DEFAULT_CONNECTION_TIMEOUT = 5  # default connection timeout in seconds
DEFAULT_PAGE_SIZE = 1000


class _FileInfo(object):
    """
    Stores info about the VMware file.
    """

    def __init__(self, path, size=None, owner=None, modification=None):
        """
        :param str path: The path to the file.
        :param int size: The size of the file in kilobytes.
        :param str owner: The user name of the owner of the file.
        :param :class:`datetime.datetime` modification: The last date and time
            the file was modified.
        """
        self.path = path
        self.size = size
        self.owner = owner
        self.modification = modification

    def __repr__(self):
        return (
            "<vmware.FileInfo: path={}, size={}>"
        ).format(self.path, self.size)


class _VMDiskInfo(object):
    """
    Stores info about the VMware virtual disk.
    """

    def __init__(
            self, disk_id, owner_id, file_path,
            size=None, label=None, summary=None, sharing=None, disk_mode=None,
            scsi_host=None, scsi_bus=None, scsi_target=None, scsi_lun=None):
        """
        :param str disk_id: Virtual disk durable and unmutable identifier.
        :param str owner_id: ID of the owner VM.
        :param str file_path : Path to the host file used in disk backing.
        :param int size: Capacity of this virtual disk in kilobytes.
        :type label: str
        :type summary: str
        :type sharing: bool
        :type disk_mode: str
        :type scsi_host: int
        :type scsi_bus: int
        :type scsi_target: int
        :type scsi_lun: int
        """
        self.disk_id = disk_id
        self.owner_id = owner_id
        self.file_path = file_path
        self.label = label
        self.size = size
        self.summary = summary
        self.sharing = sharing
        self.disk_mode = disk_mode
        self.scsi_host = scsi_host
        self.scsi_bus = scsi_bus
        self.scsi_target = scsi_target
        self.scsi_lun = scsi_lun

    @property
    def file_info(self):
        return _FileInfo(self.file_path, size=self.size)

    def __repr__(self):
        return (
            "<vmware._VMDiskInfo: disk_id={}, owner_id={}, file_path={}>"
        ).format(self.disk_id, self.owner_id, self.file_path)


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

        creation_times = self._query_node_creation_times()
        for node in nodes:
            node.created_at = creation_times.get(node.id)

        return nodes

    def list_images(self, ex_datacenter=None):
        """
        List available images (templates).
        """
        virtual_machines = self._list_virtual_machines(
            datacenter=ex_datacenter,
            is_template=True)
        images = [self._to_image(vm_image) for vm_image in virtual_machines]

        creation_times = self._query_node_creation_times()
        for image in images:
            image_id = image.extra['managed_object_id']
            image.extra['created_at'] = creation_times.get(image_id)

        return images

    def list_volumes(self, node=None):
        """
        List available volumes (on all datastores).
        """
        if node is None:
            virtual_machines = self._list_virtual_machines()
        else:
            virtual_machines = [self.ex_get_vm(node)]

        # Search for VM's virtual disks
        vm_disks = collections.defaultdict(list)
        for vm_disk in self._query_vm_virtual_disks(virtual_machines):
            vm_disks[vm_disk.file_path].append(vm_disk)

        # Search for *.vmdk files, clusterwide or on specified node
        if node is None:
            vmdk_files = self._query_datastore_files(vim.VmDiskFileQuery())
        else:
            vmdk_files = {disk[0].file_info for disk in vm_disks.values()}

        # Create ``StorageVolume`` objects
        volumes = [
            self._to_volume(vmdk_file, devices=vm_disks.get(vmdk_file.path))
            for vmdk_file in vmdk_files]

        # Update ``StorageVolume`` creation timestamps
        creation_times = self._query_volume_creation_times(
            volumes,
            virtual_machine=virtual_machines[0] if node is not None else None)
        for volume in volumes:
            volume.extra['created_at'] = creation_times.get(volume.id)

        return volumes

    def list_snapshots(self, node=None):
        """
        List available snapshots.
        """
        if node is None:
            virtual_machines = self._list_virtual_machines()
        else:
            virtual_machines = [self.ex_get_vm(node)]

        snapshots = []
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
                    if device['scsi_host'] == 7 \
                            and device['scsi_bus'] == 0 \
                            and device['scsi_lun'] == 0:
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
        content = self._retrieve_content()
        virtual_machines = []
        for child in content.rootFolder.childEntity:
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

    def _query_datastore_files(
            self,
            file_query,
            datastores=None,
            sort_folders_first=True,
            raise_on_error=True):
        """
        Returns the information for the files that match the given search
        criteria.

        :param datastores: Search for the files, across specified datastores.
        :type datastores: list[:class:`vim.Datastore`]

        :param file_query: The set of file types to match, search criteria
            specific to the file type, and the amount of detail for a file.
            This parameter must be a subclass of :class:`vim.FileQuery`
            (:class:`vim.VmDiskFileQuery`, :class:`vim.VmSnapshotFileQuery`,
            :class:`vim.VmConfigFileQuery` etc.).
        :type file_query: :class:`vim.FileQuery`

        :param sort_folders_first: By default, files are sorted in alphabetical
            order regardless of file type. If this flag is set to ``True``,
            folders are placed at the start of the list of results in
            alphabetical order. The remaining files follow in alphabetical
            order. (``True`` by default)
        :type sort_folders_first: bool

        :param raise_on_error: Any exception from VMware task thrown is
            thrown up to the caller if ``raise_on_error`` is set to ``True``.
            (``True`` by default)
        :type raise_on_error: bool

        See:
         - http://pubs.vmware.com/vi30/sdk/ReferenceGuide/vim.host.DatastoreBrowser.html
         - https://pubs.vmware.com/vsphere-51/topic/com.vmware.wssdk.apiref.doc/vim.host.DatastoreBrowser.SearchSpec.html

        :rtype: list[:class:`_FileInfo`]
        """
        if not isinstance(file_query, vim.FileQuery):
            raise Exception("Unknown type of the file query")
        if file_query.__class__ is vim.FileQuery:
            raise Exception("Too broad type of the file query")

        datastores = datastores or self._list_datastores()
        datastores_info = [
            datastore.info for datastore in datastores
        ]  # type: list[vim.host.VmfsDatastoreInfo]

        filter_query_flags = vim.FileQueryFlags(
            fileSize=True,
            fileType=True,
            fileOwner=True,
            modification=True)
        search_spec = vim.HostDatastoreBrowserSearchSpec(
            query=[file_query],
            details=filter_query_flags,
            sortFoldersFirst=sort_folders_first)
        search_tasks = (
            ds.browser.SearchSubFolders("[{}]".format(ds.name), search_spec)
            for ds in datastores)

        result = []  # type: list[_FileInfo]
        for task in search_tasks:
            vmware_task.WaitForTask(task, raiseOnError=raise_on_error)

            files = (
                (files.folderPath, info)
                for files in task.info.result
                for info in files.file)
            for folder_path, file_info in files:
                full_path = self._file_name_to_path(
                    '{}{}'.format(folder_path, file_info.path),
                    datastores_info=datastores_info)
                result.append(_FileInfo(
                    path=full_path,
                    size=int(file_info.fileSize) / 1024,
                    owner=file_info.owner,
                    modification=file_info.modification))
        return result

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

    def _to_volume(self, file_info, devices=None):
        """
        Creates :class:`StorageVolume` object from :class:`_FileInfo`.

        The volume ID format: `ds:///vmfs/volumes/<store-id>/<volume-path>.vmdk`

        The extra dict format:
         - file_path: ``str``
         - last_modified: ``None`` | :class:`datetime.datetime`
         - created_at: ``None`` | :class:`datetime.datetime`
         - devices: ``dict[str, list[dict]]``

        :param file_info: The VMDK file.
        :type file_info: :class:`_FileInfo`.
        :param devices: The list of attached devices.
        :type devices: list[:class:`_VMDiskInfo`]

        :rtype: :class:`StorageVolume`
        """
        devices = devices or []
        if len(devices) > 1 \
                and not all(device.sharing is True for device in devices):
            disks_ids = [device.disk_id for device in devices]
            raise LibcloudError((
                "Unable to create StorageVolume with multiple non-shared "
                "devices: {}."
            ).format(', '.join(disks_ids)))

        extra = {
            'file_path': file_info.path,
            'last_modified': file_info.modification,
            'created_at': None,
            'devices': collections.defaultdict(list),
        }
        for device in devices:
            extra['devices'][device.owner_id].append(device.__dict__)

        return StorageVolume(
            id=file_info.path,
            name=os.path.basename(file_info.path),
            size=file_info.size,
            driver=self,
            extra=extra)

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
            'created_at': snapshot_tree.createTime,
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
            extra={
                # pylint: disable=protected-access
                'managed_object_id': virtual_machine._GetMoId(),
            },
            driver=self)
