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
import logging
import os
import re
import ssl
import time
from datetime import datetime
from datetime import timedelta
from datetime import timezone

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

try:
    from pyVim import connect
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
DEFAULT_EVENTS_DAYS_LIMIT = 30


LOG = logging.getLogger(__name__)


class VSpherePropertyCollector(misc_utils.PageList):
    # pylint: disable=line-too-long
    """
    The paginated PropertyCollector.

    See:
     - http://pubs.vmware.com/vsphere-50/index.jsp#com.vmware.wssdk.pg.doc_50/PG_Ch5_PropertyCollector.7.2.html
    """

    def __init__(
            self, driver, object_cls,
            container=None, path_set=None,
            process_fn=None, page_size=None):
        """
        :param driver: The VSphere driver.
        :type driver: :class:`VSphereNodeDriver`

        :param object_cls: Type of managed object.
        :type object_cls: :class:`vim.ManagedEntity`

        :param container: The :class:`vim.Folder` or :class:`vim.Datacenter`
            instance that provides the objects that the view presents.
        :type container: :class:`vim.Folder` or :class:`vim.Datacenter`

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
        self._current_page_num = 0

        self._container = container
        self._container_view = None
        self._collector = None

        def _process_fn(result):
            """Convert result to list|dict before processing."""
            if result is None:
                return []
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
        return response.token if response else None

    def _retrieve_properties(self):
        self._current_page_num += 1
        start_time = time.monotonic()
        LOG.debug(
            "%s: Querying page of %ss (container=%s, properties=%s, "
            "page_num=%d)...",
            self.__class__.__name__,
            self._object_cls.__name__,
            self._container,
            self._path_set or 'all',
            self._current_page_num)

        if self.next_page_token is None:
            self._collector, retrieve_args = self._create_property_collector()
            result = self._collector.RetrievePropertiesEx(*retrieve_args)
        else:
            result = self._collector.ContinueRetrievePropertiesEx(
                token=self.next_page_token)

        LOG.debug(
            "%s: Querying page of %d %ss (container=%s, properties=%s, "
            "page_num=%d) finished in %.3f seconds.",
            self.__class__.__name__,
            len(getattr(result, 'objects', [])),
            self._object_cls.__name__,
            self._container,
            self._path_set or 'all',
            self._current_page_num,
            time.monotonic() - start_time)
        return result

    def _create_property_collector(self):
        """
        Initializes the property collector.

        :return: The property collector and paramaters for
            :meth:`vmodl.query.PropertyCollector.RetrieveProperties` method.
        :rtype: tulpe(:class:`vmodl.query.PropertyCollector`, tuple)
        """
        content = self._connection.content
        self._container_view = content.viewManager.CreateContainerView(
            container=self._container or content.rootFolder,
            type=[self._object_cls],
            recursive=True,
        )  # type: vim.view.ContainerView

        # Create a traversal specification to identify the path for
        # collection
        traversal_spec = vmodl.query.PropertyCollector.TraversalSpec(
            name='traverseEntities',
            path='view',
            skip=False,
            type=self._container_view.__class__)

        # Create an object specification to define the starting point
        # for inventory navigation
        object_spec = vmodl.query.PropertyCollector.ObjectSpec(
            obj=self._container_view,
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

    def __iter__(self):
        try:
            for item in super(VSpherePropertyCollector, self).__iter__():
                yield item
        finally:
            self.destroy()

    def destroy(self):
        """
        Destroy property collector resources.
        """
        if self._container_view:
            try:
                self._container_view.DestroyView()
                self._container_view = None
            except Exception as err:
                LOG.warning(str(err))
        self._collector = None
        self._current_page_num = 0


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

    def __init__(self, driver, file_query, datacenter=None, process_fn=None):
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
        self._datacenter = datacenter
        self._search_tasks = None  # type: Iterator

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
            if self._datacenter is not None:
                datastores = self._datacenter.datastore
            else:
                datastores = self._driver.ex_list_datastores()
            filter_query_flags = vim.FileQueryFlags(
                fileSize=True,
                fileType=True,
                fileOwner=True,
                modification=True)
            search_spec = vim.HostDatastoreBrowserSearchSpec(
                query=[self._file_query],
                details=filter_query_flags,
                sortFoldersFirst=True)

            self._search_tasks = []
            for index, datastore in enumerate(datastores):
                search_task = datastore.browser.SearchSubFolders(
                    '[{}]'.format(datastore.name),
                    search_spec)
                task_id = search_task.info.key.split(']', 1)[-1]
                LOG.debug(
                    "%s: Started SearchSubFolders task (id=%s, datastore=%s, "
                    "number=%d/%d)",
                    self.__class__.__name__,
                    task_id,
                    datastore._moId,
                    index + 1,
                    len(datastores))
                self._search_tasks.append((task_id, search_task, time.monotonic()))
            self._search_tasks = iter(self._search_tasks)

        result = []
        try:
            task_id, task, start_time = next(self._search_tasks)
        except StopIteration:
            self._search_tasks = None
            return result

        task_result = self._wait_for_task(task, task_id, start_time=start_time)
        if not task_result:
            # unable to get files
            return result

        files = (
            (files.folderPath, info)
            for files in task_result
            for info in files.file)
        for folder_path, file_info in files:
            full_path = self._driver.ex_file_name_to_path(
                name='{}{}'.format(folder_path, file_info.path))
            result.append(_FileInfo(
                path=full_path,
                size=int(file_info.fileSize) / 1024,
                owner=file_info.owner,
                modification=file_info.modification))
        LOG.debug(
            "%s: Processed %s results from SearchSubFolders task (id=%s)",
            self.__class__.__name__,
            len(result),
            task_id)
        return result

    def _wait_for_task(
            self,
            task,
            task_id=None,
            start_time=None,
            timeout=300,
            interval=0.5):
        """
        Wait for a vCenter task to finish.

        :returns: Result if task is successfully completed, otherwise it
            returns a ``None``.
        """
        start_time = start_time or time.monotonic()
        task_info = task.info
        task_id = task_id or task_info.key.split(']', 1)[-1]

        while True:
            LOG.debug(
                "%s: Waiting for SearchSubFolders task (id=%s, state=%s, running_time=%.3f "
                "sec)...",
                self.__class__.__name__,
                task_id,
                task_info.state,
                time.monotonic() - start_time)
            if time.monotonic() - start_time >= timeout:
                raise Exception((
                    "Timeout while waiting for import task ID {}"
                ).format(task_info.id))

            if task_info.state in (
                    vim.TaskInfo.State.success,
                    vim.TaskInfo.State.error):
                LOG.debug(
                    "%s: SearchSubFolders task (id=%s, state=%s) finished "
                    "in %.3f seconds",
                    self.__class__.__name__,
                    task_id,
                    task_info.state,
                    time.monotonic() - start_time)
                if task_info.state == vim.TaskInfo.State.success:
                    return task_info.result
                break
            time.sleep(interval)
            task_info = task.info


class _FileInfo(object):
    """
    Stores info about the VMware file.
    """

    def __init__(self, path, size=None, owner=None, modification=None, devices=None):
        """
        :param str path: The path to the file.
        :param int size: The size of the file in **kilobytes**.
        :param str owner: The user name of the owner of the file.
        :param :class:`datetime.datetime` modification: The last date and time
            the file was modified.
        :param list devices: The list of virtual devices associated with the
            VMDK file.
        """
        self.path = path
        self.size = size
        self.owner = owner
        self.modification = modification
        self.devices = devices or []

    @property
    def size_in_gb(self):
        if self.size is not None:
            return round(self.size / (1024.0 ** 2), 1)

    def __eq__(self, other):
        return self.path == other.path

    def __hash__(self):
        return hash(self.path)

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
        return _FileInfo(self.file_path, size=self.size, devices=[self])

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

    def __eq__(self, other):
        return self.disk_id == other.disk_id

    def __hash__(self):
        return hash(self.disk_id)

    def __repr__(self):
        return (
            "<vmware._VMDiskInfo: disk_id={}, owner_id={}, file_path={}>"
        ).format(self.disk_id, self.owner_id, self.file_path)


class VSphereConnection(ConnectionUserAndKey):
    """
    Represents a single connection to the VSphere platform.

    Note: Uses SOAP for communication.
    """

    def __init__(
            self, user_id, key,
            secure=True,
            host=None,
            port=None,
            url=None,
            timeout=None,
            disconnect_on_terminate=True,
            **kwargs):
        """
        :param disconnect_on_terminate: Closes connection automatically at the
            process termination (via :mod:`atexit` callback). For long-running
            processes, it is better to call the :meth:`disconnect` manually.
        :type disconnect_on_terminate: bool, optional
        """
        if url is not None:
            if host is not None or port is not None:
                raise ValueError(
                    "'url' and 'host'/'port' arguments are mutually "
                    "exclusive.")
            host, port, secure, sdk_path = self._tuple_from_url(url)
        else:
            sdk_path = None
        if not host:
            raise ValueError(
                "Either 'host' or 'url argument must be provided.")

        self._sdk_path = sdk_path or '/sdk'
        self._disconnect_on_terminate = disconnect_on_terminate
        self.client = None  # type: tp.Optional[vim.ServiceInstance]
        self.content = None  # type: tp.Optional[vim.ServiceInstanceContent]

        super(VSphereConnection, self).__init__(
            user_id=user_id,
            key=key, secure=secure,
            host=host, port=port,
            timeout=timeout, **kwargs)

    def connect(self, host=None, port=None, base_url=None, **kwargs):
        """
        Establish a connection with the API server.

        :param str host: Optional host to override our default.
        :param int port: Optional port to override our default.
        :param str base_url: Optional URL to override our default.
        """
        if base_url is not None:
            host, port, secure, path = self._tuple_from_url(base_url)
        else:
            host = host or self.host
            port = port or self.port
            secure, path = self.secure, self._sdk_path

        protocol = 'https' if secure else 'http'
        vcenter_url = '{}://{}:{}{}'.format(protocol, host, port, path)
        LOG.debug("Creating the vSphere (%s) session ...", vcenter_url)

        try:
            self.client = connect.SmartConnect(
                protocol=protocol,
                host=host,
                port=port,
                user=self.user_id,
                pwd=self.key,
                path=path,
                sslContext=ssl._create_unverified_context())
            self.content = self.client.RetrieveContent()
            connect.SetSi(None)  # removes connection object from the global scope
        except Exception as err:
            message = '{}'.format(err)

            if 'incorrect user name' in message:
                raise InvalidCredsError(
                    "Check that your username and password are valid.")
            if 'connection refused' in message or 'not a vim server' in message:
                raise LibcloudError((
                    "Check that the host provided ({0}) is a vSphere "
                    "installation."
                ).format(vcenter_url))
            if 'name or service not known' in message:
                raise LibcloudError((
                    "Check that the vSphere ({0}) is accessible."
                ).format(vcenter_url))

            raise LibcloudError((
                "Unable to create the vSphere ({0}) session: {1}"
            ).format(vcenter_url, message))

        if self._disconnect_on_terminate:
            atexit.register(self.disconnect)

    def disconnect(self):
        if self.client is not None:
            connect.Disconnect(self.client)
            self.client = None
        self.content = None


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

    def __init__(
            self, username, password, secure=True, host=None, port=None,
            url=None, timeout=None, disconnect_on_terminate=True,
            created_at_limit=DEFAULT_EVENTS_DAYS_LIMIT, allow_caching=False,
            **kwargs):
        """
        :param created_at_limit: The limit (in days) on the `created_at`
            datetimes, the small value may increase the listing
            performance. (optional, 30 by default)

            See:
             - :meth:`self._query_node_creation_times`
             - :meth:`self._query_volume_creation_times`
        :type created_at_limit: int | float | None

        :param allow_caching: Allow data caching for the expensive requests.
            (optional, `False` by default)
        :type allow_caching: bool
        """
        self.url = url
        self._disconnect_on_terminate = disconnect_on_terminate
        self._created_at_limit = created_at_limit

        self._allow_caching = allow_caching
        self._cache = collections.defaultdict(dict)

        super(VSphereNodeDriver, self).__init__(
            key=username, secret=password,
            secure=secure, host=host,
            port=port, url=url,
            timeout=timeout, **kwargs)

    def _set_cache(self, name, value):
        if self._allow_caching:
            self._cache[name] = value

    def _get_cache(self, name):
        return self._cache.get(name) if self._allow_caching else None

    def _has_cache(self, name):
        return name in self._cache if self._allow_caching else False

    def ex_pre_caching(
            self,
            node_creation_times=True,
            volume_creation_times=True):
        """
        Pre-loading data into cache which is needed in the future.

        :param node_creation_times: Pre-cache the nodes/images creation
            timestamps.
        :type node_creation_times: bool

        :param volume_creation_times: Pre-cache the volumes creation timestamps.
        :type volume_creation_times: bool
        """
        if not self._allow_caching:
            raise LibcloudError((
                "Caching is disabled for {0} instance, use 'allow_caching' "
                "option."
            ).format(self.__class__.__name__))

        if volume_creation_times:
            # the volumes listing requires the nodes timestamps
            node_creation_times = True
            self._query_volume_creation_times()
        if node_creation_times:
            self._query_node_creation_times()

        # required to cache
        self._get_datastores_info_map()
        self._get_datacenter_ids_map()

    def _ex_connection_class_kwargs(self):
        kwargs = {
            'url': self.url,
            'disconnect_on_terminate': self._disconnect_on_terminate
        }

        return kwargs

    def list_nodes(self, ex_datacenter=None, ex_page_size=None):
        """
        Lists available nodes (excluding templates).

        :param ex_datacenter: Filters the node list to nodes that are
            located in this datacenter. (optional)
        :type ex_datacenter: str | :class:`vim.Datacenter` | None

        :rtype: list[Node]
        """
        return list(self.iterate_nodes(
            ex_datacenter=ex_datacenter,
            ex_page_size=ex_page_size,
        ))

    def iterate_nodes(self, ex_datacenter=None, ex_page_size=None):
        """
        Iterate available nodes (excluding templates).

        :rtype: collections.Iterable[Node]
        """
        if ex_datacenter is not None:
            ex_datacenter = self._get_datacenter_by_id(ex_datacenter)
        creation_times = self._query_node_creation_times()

        def result_to_nodes(result):
            nodes = []
            for vm_entity, vm_properties in result.items():
                if not vm_properties.get('summary.config') \
                        or vm_properties['summary.config'].template is True:
                    continue
                node = self._to_node(vm_entity, vm_properties)
                if node:
                    node.created_at = creation_times.get(node.id)
                    nodes.append(node)
            return nodes

        return VSpherePropertyCollector(
            self, vim.VirtualMachine,
            path_set=[
                'config.datastoreUrl',
                'summary',
                'summary.config',
                'summary.runtime',
                'summary.guest',
            ],
            process_fn=result_to_nodes,
            page_size=ex_page_size,
            container=ex_datacenter,
        )

    def list_images(self, ex_datacenter=None, ex_page_size=None):
        """
        List available images (templates).

        :param ex_datacenter: Filters the node list to nodes that are
            located in this datacenter. (optional)
        :type ex_datacenter: str | :class:`vim.Datacenter` | None

        :rtype: list[Image]
        """
        return list(self.iterate_images(
            ex_datacenter=ex_datacenter,
            ex_page_size=ex_page_size,
        ))

    def iterate_images(self, ex_datacenter=None, ex_page_size=None):
        """
        Iterate available images (templates).

        :rtype: collections.Iterable[Image]
        """
        if ex_datacenter is not None:
            ex_datacenter = self._get_datacenter_by_id(ex_datacenter)
        creation_times = self._query_node_creation_times()

        def result_to_images(result):
            images = []
            for vm_entity, vm_properties in result.items():
                if not vm_properties.get('summary.config') \
                        or vm_properties['summary.config'].template is False:
                    continue
                image = self._to_image(vm_entity, vm_properties)
                if image:
                    image.created_at = creation_times.get(
                        image.extra['managed_object_id'])
                    images.append(image)
            return images

        return VSpherePropertyCollector(
            self, vim.VirtualMachine,
            path_set=[
                'config.datastoreUrl',
                'summary.config',
            ],
            process_fn=result_to_images,
            page_size=ex_page_size,
            container=ex_datacenter,
        )

    def list_volumes(self, node=None, ex_datacenter=None):
        """
        List available volumes (on all datastores).

        :param ex_datacenter: Filters the volume list to volume that are
            located in this datacenter. (optional)
        :type ex_datacenter: str | :class:`vim.Datacenter` | None

        :rtype: list[StorageVolume]
        """
        return list(self.iterate_volumes(node=node, ex_datacenter=ex_datacenter))

    def iterate_volumes(self, node=None, ex_datacenter=None):
        """
        Iterate available volumes (on all datastores).

        :rtype: collections.Iterable[StorageVolume]
        """
        if node is not None:
            if ex_datacenter:
                raise ValueError(
                    "Cannot list the volumes for the datacenter and the "
                    "virtual machine at the same time")
            virtual_machine = self.ex_get_vm(node)
        else:
            virtual_machine = None

        if ex_datacenter is not None:
            ex_datacenter = self._get_datacenter_by_id(ex_datacenter)

        # querying the creation timestamps of node(s) and volumes
        node_creation_times = self._query_node_creation_times(
            virtual_machine=virtual_machine)
        volume_creation_times = self._query_volume_creation_times(
            virtual_machine=virtual_machine)

        shared_files = collections.defaultdict(list)

        def result_to_volumes(files_info, allow_shared=False):
            """
            :type disks_page: tp.Union[tp.List[_FileInfo], tp.List[_VMDiskInfo]]
            :rtype: tp.List[StorageVolume]
            """
            if files_info and isinstance(files_info[0], _VMDiskInfo):
                files_info = (disk.file_info for disk in files_info)

            volumes = []
            for file_info in files_info:

                if not allow_shared and any(
                        d.sharing
                        for d in file_info.devices):
                    shared_files[file_info.path].append(file_info)
                    continue

                try:
                    volume = self._to_volume(file_info)
                except LibcloudError as err:
                    # one broken volume should not break the whole iteration
                    LOG.warning(str(err))
                    continue

                created_at = volume_creation_times.get(volume.id)
                for device in file_info.devices:
                    if created_at:
                        break
                    if device.is_root:
                        created_at = node_creation_times.get(device.owner_id)
                volume.extra['created_at'] = created_at

                volumes.append(volume)
            return volumes

        for item in self._query_vm_virtual_disks(
                virtual_machine=virtual_machine,
                datacenter=ex_datacenter,
                process_fn=result_to_volumes):
            yield item

        # collect and yield the shared volumes at the end of iteration
        merged_shared_files = []
        for files_info in shared_files.values():
            files_info[0].devices = list({
                device for file_info in files_info
                for device in file_info.devices})
            merged_shared_files.append(files_info[0])
        for item in result_to_volumes(merged_shared_files, allow_shared=True):
            yield item

    def list_snapshots(self, ex_datacenter=None, ex_page_size=None):
        """
        List available snapshots.

        :param ex_datacenter: Filters the snapshots list to snapshots that are
            located in this datacenter. (optional)
        :type ex_datacenter: str | :class:`vim.Datacenter` | None

        :rtype: list[VolumeSnapshot]
        """
        return list(self.iterate_snapshots(
            ex_datacenter=ex_datacenter,
            ex_page_size=ex_page_size,
        ))

    def iterate_snapshots(self, ex_datacenter=None, ex_page_size=None):
        """
        Iterate available snapshots.

        :rtype: collections.Iterable[VolumeSnapshot]
        """
        if ex_datacenter is not None:
            ex_datacenter = self._get_datacenter_by_id(ex_datacenter)

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
                'config.datastoreUrl',
                'snapshot.rootSnapshotList',
                'layoutEx.snapshot',
                'layoutEx.file',
            ],
            process_fn=result_to_snapshots,
            page_size=ex_page_size,
            container=ex_datacenter,
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

    def _get_datacenter_by_id(self, datacenter_id):
        """
        Returns the VMWare datacenter for a given ID.

        :param datacenter_id: The ID of the datacenter.
        :type datacenter_id: str

        :rtype: :class:`vim.Datacenter`
        """
        if isinstance(datacenter_id, vim.Datacenter):
            return datacenter_id
        datacenter_ids = self._get_datacenter_ids_map()
        if datacenter_id not in datacenter_ids:
            raise ValueError((
                "Unknown datacenter ID '{}', available: {}"
            ).format(datacenter_id, ', '.join(datacenter_ids.keys())))
        return datacenter_ids[datacenter_id]

    def _get_datastores_info_map(self):
        """
        Returns the datastore info to datacenter map.

        See: https://pubs.vmware.com/vi30/sdk/ReferenceGuide/vim.host.VmfsDatastoreInfo.html

        Note: This method is used during iteration in `self.iter_*` methods,
            so the result will be cached ignoring `self._allow_caching` option.

        :rtype: dict[:class:`vim.VmfsDatastoreInfo`, :class:`vim.Datacenter`]
        """
        if 'datastores_info_map' not in self._cache:
            self._cache['datastores_info_map'] = {
                datastore.info: datacenter
                for datacenter in self.ex_list_datacenters()
                for datastore in datacenter.datastore}
        return self._cache['datastores_info_map']

    def _get_datacenter_ids_map(self):
        """
        Returns the datacenter ID to datacenter object map.

        Note: This method is used during iteration in `self.iter_*` methods,
            so the result will be cached ignoring `self._allow_caching` option.

        :rtype: dict[str, :class:`vim.Datacenter`]
        """
        if 'datacenter_ids_map' not in self._cache:
            # pylint: disable=protected-access
            self._cache['datacenter_ids_map'] = {
                datacenter._moId: datacenter
                for datacenter in self.ex_list_datacenters()}
        return self._cache['datacenter_ids_map']

    def _get_datacenter_id_by_url(self, url):
        """
        Returns the datacenter ID for a given datastore URL.

        Example URLs:
         - `/vmfs/volumes/599d9c57-a93b0b7a-ea4c-984be16496c6`
         - `ds:///vmfs/volumes/599d9c57-a93b0b7a-ea4c-984be16496c6/volume.vmdk`

        :raise :class:`LibcloudError`: When volume URL format is invalid.

        :type url: str | :class:`vim.vm.ConfigInfo.DatastoreUrlPair` | list
        :rtype: str | None
        """
        if url and isinstance(url, list):
            url = url[0]
        if isinstance(url, vim.vm.ConfigInfo.DatastoreUrlPair):
            url = str(url.url)
        if not url:
            return

        try:
            url = url.split('/')
            volume_id = url[url.index('volumes') + 1]
        except (ValueError, IndexError):
            raise LibcloudError("Unexpected URL format: {}".format(url))

        datastore_url = 'ds:///vmfs/volumes/{}/'.format(volume_id)
        for info, datacenter in self._get_datastores_info_map().items():
            if info.url == datastore_url:
                return datacenter._moId

    def _query_node_creation_times(self, virtual_machine=None):
        """
        Fetches the creation timestamps of the VMs from the event history.

        :type virtual_machine: :class:`vim.VirtualMachine`
        :rtype: dict[str, :class:`datetime.datetime`]
        """
        if not self._created_at_limit:
            return {}

        if self._has_cache('node_creation_times'):
            return self._get_cache('node_creation_times')

        created_events = self._query_events(
            event_type_id=[
                'VmBeingDeployedEvent',
                'VmCreatedEvent',
                'VmRegisteredEvent',
                'VmClonedEvent'
            ],
            entity=virtual_machine,
            begin_time=datetime.now(timezone.utc) - timedelta(days=self._created_at_limit),
        )  # type: list[vim.Event]

        node_creation_times = {
            # pylint: disable=protected-access
            event.vm.vm._moId: event.createdTime
            for event in created_events
            if event.vm is not None}

        if not virtual_machine:
            # per-vm request is lightweight, there's no sense in caching
            self._set_cache('node_creation_times', node_creation_times)

        return node_creation_times

    def _query_volume_creation_times(self, virtual_machine=None):
        """
        Fetches the creation timestamps of the extra volumes
        from the event history.

        :type virtual_machine: :class:`vim.VirtualMachine`
        :rtype: dict[str, :class:`datetime.datetime`]
        """
        if not self._created_at_limit:
            return {}

        if self._has_cache('volume_creation_times'):
            return self._get_cache('volume_creation_times')

        reconfigure_events = self._query_events(
            event_type_id='VmReconfiguredEvent',
            entity=virtual_machine,
            begin_time=datetime.now(timezone.utc) - timedelta(days=self._created_at_limit)
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

        if not virtual_machine:
            # per-vm request is lightweight, there's no sense in caching
            self._set_cache('volume_creation_times', volume_creation_times)

        return volume_creation_times

    def ex_get_vm(self, node_or_uuid):
        """
        Searches VMs for a given instance_uuid or Node object.

        :type node_or_uuid: :class:`Node` | str
        :rtype: :class:`vim.VirtualMachine`
        """
        if isinstance(node_or_uuid, Node):
            node_or_uuid = node_or_uuid.extra['instance_uuid']
        vm = self.connection.content.searchIndex.FindByUuid(
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

    def ex_file_name_to_path(self, name):
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
            raise LibcloudError("Unexpected file name format: {}".format(name))
        datastore_name, file_path = match.group(1, 2)

        for datastore_info in self._get_datastores_info_map():
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
        :type begin_time: :class:`datetime.datetime`

        :param end_time: The end of the time range. If this property is not
            specified, then events are collected up to the latest
            time in the database. (optional)
        :type end_time: :class:`datetime.datetime`

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

        content = self.connection.content
        history_collector = content.eventManager.CreateCollectorForEvents(
            filter_spec)  # type: vim.event.EventHistoryCollector
        history_collector.SetCollectorPageSize(page_size)
        history_collector.ResetCollector()
        current_page_num = 0
        events = []

        while events or not current_page_num:
            current_page_num += 1
            start_time = time.monotonic()
            LOG.debug(
                "%s: Querying page of vim.Events (types=%s, begin_time=%s, page_num=%d) ...",
                self.__class__.__name__,
                event_type_id,
                begin_time,
                current_page_num)

            events = history_collector.ReadPreviousEvents(
                page_size
            ) if current_page_num != 1 else history_collector.latestPage

            LOG.debug(
                "%s: Querying page of %d vim.Events (types=%s, begin_time=%s, page_num=%d) "
                "finished in %.3f seconds.",
                self.__class__.__name__,
                len(events),
                event_type_id,
                begin_time,
                current_page_num,
                time.monotonic() - start_time)

            for event in events:
                yield event

        try:
            # try to delete EventHistoryCollector properly
            history_collector.DestroyCollector()
        except Exception as err:
            LOG.warning(str(err))

    def _query_vm_virtual_disks(
            self,
            virtual_machine=None,
            datacenter=None,
            process_fn=None):
        """
        Return properties of :class:`vim.vm.device.VirtualDisk`.

        See:
         - https://pubs.vmware.com/vi3/sdk/ReferenceGuide/vim.vm.device.VirtualDisk.html

        :param virtual_machine: The virtual machine.
        :type virtual_machine: :class:`vim.VirtualMachine`

        :rtype: tp.Iterable[:class:`_VMDiskInfo`]
        """
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
                file_path = self.ex_file_name_to_path(name=backing.fileName)
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
            return process_fn(disks_info) if process_fn else disks_info

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
            container=datacenter,
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

        :param vm_properties: VM properties.
        :type vm_properties: dict
        """
        if vm_properties is not None:
            datastore_url = vm_properties.get('config.datastoreUrl')
            summary = vm_properties.get('summary')
            config = vm_properties.get('summary.config')
            runtime = vm_properties.get('summary.runtime')
            guest = vm_properties.get('summary.guest')
        else:
            datastore_url = getattr(vm_entity.config, 'datastoreUrl', None)
            summary = vm_entity.summary
            config = summary.config
            runtime = summary.runtime
            guest = summary.guest

        extra = {
            'uuid': getattr(config, 'uuid', None),
            'instance_uuid': getattr(config, 'instanceUuid', None),
            'path': getattr(config, 'vmPathName', None),
            'guest_id': getattr(config, 'guestId', None),
            'template': getattr(config, 'template', None),
            'overall_status': getattr(summary, 'overallStatus', None),
            'operating_system': getattr(config, 'guestFullName', None),
            'cpus': getattr(config, 'numCpu', None),
            'memory_mb': getattr(config, 'memorySizeMB', None),
            'boot_time': None,
            'annotation': None,
            'datacenter': self._get_datacenter_id_by_url(datastore_url),
            'custom_value': {
                item.key: item.value
                for item in getattr(summary, 'customValue', {})
            }
        }

        boot_time = getattr(runtime, 'bootTime', None)
        if boot_time:
            extra['boot_time'] = boot_time.isoformat()

        annotation = getattr(config, 'annotation', None)
        if annotation:
            extra['annotation'] = annotation

        public_ips = []
        private_ips = []
        if getattr(guest, 'ipAddress', None):
            ip_addr = u'{}'.format(guest.ipAddress)
            if networking.is_valid_ipv4_address(ip_addr):
                if networking.is_public_subnet(ip_addr):
                    public_ips.append(ip_addr)
                else:
                    private_ips.append(ip_addr)

        state = self.NODE_STATE_MAP.get(
            getattr(runtime, 'powerState', None),
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
        if vm_properties is not None:
            datastore_url = vm_properties.get('config.datastoreUrl')
            config = vm_properties.get('summary.config')
        else:
            datastore_url = getattr(vm_entity.config, 'datastoreUrl', None)
            config = vm_entity.summary.config

        return NodeImage(
            id=getattr(config, 'uuid', None),
            name=getattr(config, 'name', None),
            extra={
                # pylint: disable=protected-access
                'managed_object_id': vm_entity._GetMoId(),
                'datacenter': self._get_datacenter_id_by_url(datastore_url),
            },
            driver=self)

    def _to_volume(self, file_info):
        """
        Creates :class:`StorageVolume` object from :class:`_FileInfo`.

        The volume ID format: `ds:///vmfs/volumes/<store-id>/<volume-path>.vmdk`

        The extra dict format:
         - file_path: ``str``
         - last_modified: ``None`` | :class:`datetime.datetime`
         - created_at: ``None`` | :class:`datetime.datetime`
         - devices: ``dict[str, list[dict]]``

        :param file_info: The VMDK file info.
        :type file_info: :class:`_FileInfo`.

        :raise :class:`LibcloudError`: When creating volume with multiple
            non-shared devices.
        :raise :class:`LibcloudError`: When volume URL format is invalid.

        :rtype: :class:`StorageVolume`
        """
        if len(file_info.devices) > 1 and not all(
                device.sharing is True
                for device in file_info.devices):
            disks_ids = [device.disk_id for device in file_info.devices]
            raise LibcloudError((
                "Unable to create StorageVolume with multiple non-shared "
                "devices: {}."
            ).format(', '.join(disks_ids)))

        extra = {
            'file_path': file_info.path,
            'last_modified': file_info.modification,
            'created_at': None,
            'devices': collections.defaultdict(list),
            'datacenter': self._get_datacenter_id_by_url(file_info.path)
        }
        for device in file_info.devices:
            extra['devices'][device.owner_id].append(device.__dict__)

        return StorageVolume(
            id=file_info.path,
            name=os.path.basename(file_info.path),
            size=file_info.size_in_gb,
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
        datastore_url = vm_properties.get('config.datastoreUrl') \
            or vm_entity.config.datastoreUrl
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
            'datacenter': self._get_datacenter_id_by_url(datastore_url),
            # pylint: disable=protected-access
            'owner_id': snapshot_tree.vm._GetMoId()
        }

        if snapshot_layout is None or file_layout is None:
            capacity_in_gb = None
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

    def ex_list_custom_fields(self):
        """Returns custom fields
        """
        custom_fields = {}
        for field_def in self.connection.content.customFieldsManager.field:
            if field_def.managedObjectType is None:
                continue
            object_type = field_def.managedObjectType.__name__.rsplit('vim.')[-1]
            custom_fields.setdefault(object_type, {})
            custom_fields[object_type][field_def.key] = field_def.name
        return custom_fields
