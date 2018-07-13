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
import os
import re
import ssl
import time
from datetime import datetime
from datetime import timedelta

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
from libcloud.utils import misc as misc_utils
from libcloud.utils import networking
from libcloud.utils.py3 import urlparse

try:
    from pyVim import connect
    from pyVmomi import vmodl
    from pyVmomi import vim
    from pyVmomi import vmodl
except ImportError:  # pragma: no cover
    raise ImportError(
        "Missing 'pyvmomi' dependency. You can install it "
        "using pip - 'pip install pyvmomi'")


__all__ = [
    'VSphereNodeDriver',
]


DEFAULT_CONNECTION_TIMEOUT = 5  # default connection timeout in seconds
DEFAULT_PAGE_SIZE = 1000

"""
VMWare vCenter Server stores events in the database for a limited period.
The default number of days to retain event messages in the database is 30.
"""
EVENTS_DAYS_LIMIT = 30


class VSpherePropertyCollector(misc_utils.PageList):
    # pylint: disable=line-too-long
    """
    The paginated PropertyCollector.

    See:
     - http://pubs.vmware.com/vsphere-50/index.jsp#com.vmware.wssdk.pg.doc_50/PG_Ch5_PropertyCollector.7.2.html
    """

    def __init__(
            self, driver, object_cls, path_set=None,
            process_fn=None, page_size=None):
        """
        :param driver: The VSphere driver.
        :type driver: :class:`VSphereNodeDriver`

        :param object_cls: Type of managed object.
        :type object_cls: :class:`vim.ManagedEntity`

        :param path_set: List of properties to retrieve, defaults to ``None``.
        :type path_set: list or None

        :param page_size: The size of the result page,
            defaults to ``DEFAULT_PAGE_SIZE``.
        :param page_size: int or None

        :param process_fn: Function to be applied to the page result,
            defaults to ``None``.
        :param process_fn: callable or None
        """
        self._connection = driver.connection
        self._object_cls = object_cls
        self._path_set = path_set
        self._collector = None

        def _process_fn(result):
            """Convert result to list|dict before processing."""
            if path_set is None:
                result = [prop.obj for prop in result.objects]
            else:
                result = {
                    prop.obj: {prop.name: prop.val for prop in prop.propSet}
                    for prop in result.objects}
            if isinstance(result, dict) and process_fn is None:
                raise LibcloudError(
                    "VSpherePropertyCollector: 'process_fn' parameter "
                    "must be specified.")
            return process_fn(result) if process_fn else result

        super(VSpherePropertyCollector, self).__init__(
            request_fn=self._retrieve_properties,
            process_fn=_process_fn,
            page_size=page_size or DEFAULT_PAGE_SIZE,
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
        content = self._connection.client.RetrieveContent()
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

        return content.propertyCollector, ([filter_spec], options)


class VCenterFileSearch(misc_utils.PageList):
    # pylint: disable=line-too-long
    """
    Search for the files that match the given search criteria.

    See:
        - http://pubs.vmware.com/vi30/sdk/ReferenceGuide/vim.host.DatastoreBrowser.html
        - https://pubs.vmware.com/vsphere-51/topic/com.vmware.wssdk.apiref.doc/vim.host.DatastoreBrowser.SearchSpec.html

    Returns a list of :class:`_FileInfo` paginated by datastores,
    one datastore represents the one page of results.
    """

    def __init__(self, driver, file_query, process_fn=None):
        """
        :param driver: The VSphere driver.
        :type driver: :class:`VSphereNodeDriver`

        :param file_query: The set of file types to match, search criteria
            specific to the file type, and the amount of detail for a file.
            This parameter must be a subclass of :class:`vim.FileQuery`
            (:class:`vim.VmDiskFileQuery`, :class:`vim.VmSnapshotFileQuery`,
            :class:`vim.VmConfigFileQuery` etc.).
        :type file_query: :class:`vim.FileQuery`

        :param process_fn: Function to be applied to result page,
            defaults to ``None``.
        :param process_fn: callable or None
        """
        if not isinstance(file_query, vim.FileQuery):
            raise LibcloudError("Unknown type of the file query")
        if file_query.__class__ is vim.FileQuery:
            raise LibcloudError("Too broad type of the file query")

        self._driver = driver  # type: VSphereNodeDriver
        self._file_query = file_query  # type: vim.FileQuery
        self._search_tasks = None  # type: Iterator
        self._datastores_info = None  # type: list[vim.host.VmfsDatastoreInfo]

        super(VCenterFileSearch, self).__init__(
            request_fn=self._retrieve_files,
            process_fn=process_fn,
            page_size=None,
            request_args=[],
            request_kwargs={})

    def extract_next_page_token(self, response):
        # return None if there are no more pages to request
        return 1 if self._search_tasks is not None else None

    def _retrieve_files(self):
        """
        Retrieves the files the datastores.

        :returns: The list of files from one datastore.
        :rtype: list[:class:`_FileInfo`]
        """
        if self._search_tasks is None:
            datastores = self._driver.ex_list_datastores()
            datastores_info = [datastore.info for datastore in datastores]
            filter_query_flags = vim.FileQueryFlags(
                fileSize=True,
                fileType=True,
                fileOwner=True,
                modification=True)
            search_spec = vim.HostDatastoreBrowserSearchSpec(
                query=[self._file_query],
                details=filter_query_flags,
                sortFoldersFirst=True)

            self._search_tasks = (
                ds.browser.SearchSubFolders('[{}]'.format(ds.name), search_spec)
                for ds in datastores)
            self._datastores_info = datastores_info

        result = []
        try:
            task = next(self._search_tasks)
        except StopIteration:
            self._search_tasks = None
            self._datastores_info = None
            return result

        if not self._wait_for_task(task, interval=0.4):
            # unable to get files
            return result

        files = (
            (files.folderPath, info)
            for files in task.info.result
            for info in files.file)
        for folder_path, file_info in files:
            full_path = self._driver.ex_file_name_to_path(
                name='{}{}'.format(folder_path, file_info.path),
                datastores_info=self._datastores_info)
            result.append(_FileInfo(
                path=full_path,
                size=int(file_info.fileSize) / 1024,
                owner=file_info.owner,
                modification=file_info.modification))
        return result

    def _wait_for_task(self, task, timeout=1800, interval=10):
        """
        Wait for a vCenter task to finish.

        :returns: Result if task is successfully completed, otherwise it
            returns a ``None``.
        """
        start_time = time.time()
        while True:
            if time.time() - start_time >= timeout:
                raise Exception((
                    "Timeout while waiting for import task ID {}"
                ).format(task.info.id))
            if task.info.state == vim.TaskInfo.State.success:
                return task.info.result
            if task.info.state == vim.TaskInfo.State.error:
                return
            time.sleep(interval)


class _FileInfo(object):
    """
    Stores info about the VMware file.
    """

    def __init__(self, path, size=None, owner=None, modification=None):
        """
        :param str path: The path to the file.
        :param int size: The size of the file in **kilobytes**.
        :param str owner: The user name of the owner of the file.
        :param :class:`datetime.datetime` modification: The last date and time
            the file was modified.
        """
        self.path = path
        self.size = size
        self.owner = owner
        self.modification = modification

    @property
    def size_in_gb(self):
        return self.size / (1024.0 ** 2)

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
        :param int size: Capacity of this virtual disk in **kilobytes**.
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
        """
        :rtype: :class:`_FileInfo`
        """
        return _FileInfo(self.file_path, size=self.size)

    @property
    def is_root(self):
        """
        :rtype: bool
        """
        # The default SCSI controller is numbered as 0. When you create a
        # virtual machine, the default hard disk is assigned to the default
        # SCSI controller 0 at bus node (0:0).
        # By default, the SCSI controller is assigned to virtual device
        # node (z:7).
        return self.scsi_host == 7 \
            and self.scsi_bus == 0 \
            and self.scsi_lun == 0

    def __repr__(self):
        return (
            "<vmware._VMDiskInfo: disk_id={}, owner_id={}, file_path={}>"
        ).format(self.disk_id, self.owner_id, self.file_path)


class VSphereConnection(ConnectionUserAndKey):
    """
    Represents a single connection to the VSphere platform.

    Note: Uses SOAP for communication.
    """

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
                raise InvalidCredsError(
                    "Check that your username and password are valid.")
            if 'connection refused' in message or 'not a vim server' in message:
                raise LibcloudError(
                    "Check that the host provided is a vSphere installation.")
            if 'name or service not known' in message:
                raise LibcloudError(
                    "Check that the vSphere host is accessible.")

        atexit.register(connect.Disconnect, self.client)


class VSphereNodeDriver(NodeDriver):
    """
    VMWare VSphere node driver.
    """
    name = "VMware vSphere"
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

        :rtype: list[Node]
        """
        return list(self.iterate_nodes(
            ex_page_size=ex_page_size,
        ))

    def iterate_nodes(self, ex_page_size=None):
        """
        Iterate available nodes (excluding templates).

        :rtype: collections.Iterable[Node]
        """
        creation_times = self._query_node_creation_times()

        def result_to_nodes(result):
            nodes = []
            for vm_entity, vm_properties in result.items():
                if vm_properties['summary.config'].template is True:
                    continue
                node = self._to_node(vm_entity, vm_properties)
                node.created_at = creation_times.get(node.id)
                nodes.append(node)
            return nodes

        return VSpherePropertyCollector(
            self, vim.VirtualMachine,
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

        :rtype: list[Image]
        """
        return list(self.iterate_images(
            ex_page_size=ex_page_size,
        ))

    def iterate_images(self, ex_page_size=None):
        """
        Iterate available images (templates).

        :rtype: collections.Iterable[Image]
        """
        creation_times = self._query_node_creation_times()

        def result_to_images(result):
            images = []
            for vm_entity, vm_properties in result.items():
                if vm_properties['summary.config'].template is False:
                    continue
                image = self._to_image(vm_entity, vm_properties)
                image.created_at = creation_times.get(
                    image.extra['managed_object_id'])
                images.append(image)
            return images

        return VSpherePropertyCollector(
            self, vim.VirtualMachine,
            path_set=[
                'summary.config',
            ],
            process_fn=result_to_images,
            page_size=ex_page_size,
        )

    def list_volumes(self, node=None):
        """
        List available volumes (on all datastores).

        :rtype: list[StorageVolume]
        """
        return list(self.iterate_volumes(node=node))

    def iterate_volumes(self, node=None):
        """
        Iterate available volumes (on all datastores).

        :rtype: collections.Iterable[StorageVolume]
        """
        virtual_machine = self.ex_get_vm(node) if node is not None else None

        # querying the virtual disks of node(s)
        virtual_disks = collections.defaultdict(list)
        for disk in self._query_vm_virtual_disks(
                virtual_machine=virtual_machine):
            virtual_disks[disk.file_path].append(disk)

        # querying the creation timestamps of node(s) and volumes
        node_creation_times = self._query_node_creation_times(
            virtual_machine=virtual_machine)
        volume_creation_times = self._query_volume_creation_times(
            virtual_machine=virtual_machine)

        def files_to_volumes(vmdk_files):
            """
            Converts a list of VMDK files to :class:`StorageVolume` objects.
            """
            volumes = []
            for file_info in vmdk_files:
                devices = virtual_disks.get(file_info.path) or []
                volume = self._to_volume(file_info, devices=devices)

                created_at = volume_creation_times.get(volume.id)
                for device in devices:
                    if created_at:
                        break
                    if device.is_root:
                        created_at = node_creation_times.get(device.owner_id)
                volume.extra['created_at'] = created_at

                volumes.append(volume)
            return volumes

        if virtual_machine:
            # iterate over disks on the one virtual machine
            vmdk_files = {disk[0].file_info for disk in virtual_disks.values()}
            return iter(files_to_volumes(vmdk_files))

        return iter(VCenterFileSearch(
            self, vim.VmDiskFileQuery(),
            process_fn=files_to_volumes))

    def list_snapshots(self, ex_page_size=None):
        """
        List available snapshots.

        :rtype: list[VolumeSnapshot]
        """
        return list(self.iterate_snapshots(
            ex_page_size=ex_page_size,
        ))

    def iterate_snapshots(self, ex_page_size=None):
        """
        Iterate available snapshots.

        :rtype: collections.Iterable[VolumeSnapshot]
        """
        def result_to_snapshots(result):
            snapshots = []
            for vm_properties in result.values():
                snapshot_trees = vm_properties.get('snapshot.rootSnapshotList')
                snapshots.extend([
                    self._to_snapshot(snap_tree, vm_properties)
                    for snap_tree in self._walk_snapshot_tree(snapshot_trees)])
            return snapshots

        return VSpherePropertyCollector(
            self, vim.VirtualMachine,
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
        Fetches the creation timestamps of the VMs from the event history.

        :type virtual_machine: :class:`vim.VirtualMachine`
        :rtype: dict[str, :class:`datetime.datetime`]
        """
        created_events = self._query_events(
            event_type_id=[
                'VmBeingDeployedEvent',
                'VmCreatedEvent',
                'VmRegisteredEvent',
                'VmClonedEvent'
            ],
            entity=virtual_machine,
            begin_time=datetime.now() - timedelta(days=EVENTS_DAYS_LIMIT)
        )  # type: list[vim.Event]
        return {
            # pylint: disable=protected-access
            event.vm.vm._GetMoId(): event.createdTime
            for event in created_events}

    def _query_volume_creation_times(self, virtual_machine=None):
        """
        Fetches the creation timestamps of the extra volumes
        from the event history.

        :type virtual_machine: :class:`vim.VirtualMachine`
        :rtype: dict[str, :class:`datetime.datetime`]
        """
        reconfigure_events = self._query_events(
            event_type_id='VmReconfiguredEvent',
            entity=virtual_machine,
            begin_time=datetime.now() - timedelta(days=EVENTS_DAYS_LIMIT)
        )  # type: list[vim.Event]

        volume_creation_times = {}  # type: dict[str, datetime.datetime]
        for event in reconfigure_events:  # lazy iterator with API pagination
            created_files = (
                change.device.backing.fileName
                for change in event.configSpec.deviceChange
                if isinstance(change.device, vim.vm.device.VirtualDisk) \
                    and change.operation == 'add' \
                    and change.fileOperation == 'create')
            volume_creation_times.update({
                created_file: event.createdTime
                for created_file in created_files})
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

    def ex_list_datacenters(self):
        """
        Returns list of datacenters.

        See: https://pubs.vmware.com/vi3/sdk/ReferenceGuide/vim.Datacenter.html

        :rtype: list[:class:`vim.Datacenter`]
        """
        return list(VSpherePropertyCollector(self, vim.Datacenter))

    def ex_list_datastores(self):
        """
        Returns the list of datastores.

        See: https://pubs.vmware.com/vi3/sdk/ReferenceGuide/vim.Datastore.html

        :rtype: list[:class:`vim.Datastore`]
        """
        return list(VSpherePropertyCollector(self, vim.Datastore))

    def ex_list_datastores_info(self):
        """
        Returns the list of info about all datastores.

        See: https://pubs.vmware.com/vi30/sdk/ReferenceGuide/vim.host.VmfsDatastoreInfo.html

        :rtype: list[:class:`vim.VmfsDatastoreInfo`]
        """
        datastores = VSpherePropertyCollector(
            self, vim.Datastore,
            path_set=['info'],
            process_fn=lambda res: [result['info'] for result in res.values()])
        return list(datastores)

    def _retrieve_content(self):
        """
        Retrieves the properties of the service instance.

        See: https://pubs.vmware.com/vi3/sdk/ReferenceGuide/vim.ServiceInstanceContent.html

        :rtype: :class:`vim.ServiceInstanceContent`
        """
        return self.connection.client.RetrieveContent()

    def ex_file_name_to_path(self, name, datastores_info):
        """
        Converts file name to full path.

        Example:
          name: ``[ds.n1.c1.dc1] base-centos7.vmdk``
          return: ``ds:///vmfs/volumes/<datastore-id>/base-centos7.vmdk``

        :type name: str
        :type datastores: list[:class:`vim.Datastore`]
        """
        match = re.match(r'^\[(.*?)\] ((\w|\W)*)$', name)
        if not match:
            raise LibcloudError("Unecpected file name format: {}".format(name))
        datastore_name, file_path = match.group(1, 2)

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
            system_user=None,
            page_size=DEFAULT_PAGE_SIZE):
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
        history_collector.SetCollectorPageSize(page_size)
        history_collector.ResetCollector()

        events = history_collector.latestPage
        while events:
            for event in events:
                yield event
            events = history_collector.ReadPreviousEvents(page_size)

    def _query_vm_virtual_disks(self, virtual_machine=None):
        """
        Return properties of :class:`vim.vm.device.VirtualDisk`.

        See:
         - https://pubs.vmware.com/vi3/sdk/ReferenceGuide/vim.vm.device.VirtualDisk.html

        :param virtual_machine: The virtual machine.
        :type virtual_machine: :class:`vim.VirtualMachine`

        :rtype: list[:class:`_VMDiskInfo`]
        """
        datastores_info = self.ex_list_datastores_info()

        def result_to_disks(result):
            disks_info = []
            vm_devices = []
            for vm_entity, vm_properties in result.items():
                vm_id = vm_entity._GetMoId()  # pylint: disable=protected-access
                for device in vm_properties.get('config.hardware.device') or []:
                    vm_devices.append((vm_id, device))
            virtual_disks = {
                (vm_id, entity) for vm_id, entity in vm_devices
                if isinstance(entity, vim.vm.device.VirtualDisk)}
            scsi_controller_to_device_map = {
                device: {
                    'unit_number': entity.scsiCtlrUnitNumber,
                    'bus_number': entity.busNumber}
                for vm_id, entity in vm_devices
                if isinstance(entity, vim.vm.device.VirtualSCSIController)
                for device in entity.device}

            for vm_id, disk in virtual_disks:
                backing = disk.backing
                device_info = disk.deviceInfo
                if not backing:
                    continue
                file_path = self.ex_file_name_to_path(
                    name=backing.fileName,
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
                disks_info.append(disk_info)
            return disks_info

        if virtual_machine is not None:
            config = virtual_machine.config
            if not config:
                return iter(())
            result = {
                virtual_machine: {
                    'config.hardware.device': config.hardware.device
                }}
            return result_to_disks(result)

        return VSpherePropertyCollector(
            self, vim.VirtualMachine,
            path_set=[
                'config.hardware.device',
            ],
            process_fn=result_to_disks,
        )

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
            ip_addr = u'{}'.format(guest.ipAddress)
            if networking.is_valid_ipv4_address(ip_addr):
                if networking.is_public_subnet(ip_addr):
                    public_ips.append(ip_addr)
                else:
                    private_ips.append(ip_addr)

        state = self.NODE_STATE_MAP.get(
            runtime.powerState,
            NodeState.UNKNOWN)

        node = Node(
            id=vm_entity._GetMoId(),  # pylint: disable=protected-access
            name=config.name,
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
            size=round(file_info.size_in_gb, 1),
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
            capacity_in_gb = sum(
                disk.size for disk in file_layout
                if disk.key in snapshot_data_keys and disk.type == 'snapshotData'
            ) / (1024.0 ** 3)

        return VolumeSnapshot(
            id=snapshot_tree.id,
            name=snapshot_tree.name,
            driver=self,
            size=round(capacity_in_gb, 1) if capacity_in_gb else None,
            extra=extra,
            created=snapshot_tree.createTime,
            state=None)
