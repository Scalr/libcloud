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
import ssl
try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

try:
    from pyVim import connect
    from pyVmomi import vmodl
except ImportError:
    raise ImportError('Missing "pyvmomi" dependency. You can install it '
                      'using pip - pip install pyvmomi')

from libcloud.common.base import ConnectionUserAndKey
from libcloud.common.types import LibcloudError
from libcloud.common.types import InvalidCredsError
from libcloud.compute.base import NodeDriver
from libcloud.compute.base import NodeImage
from libcloud.compute.base import Node
from libcloud.compute.base import StorageVolume
from libcloud.compute.types import NodeState, Provider
from libcloud.utils.networking import is_public_subnet


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

    def list_images(self, ex_datacenter=None):
        """
        List available images (templates).
        """
        images = []
        for node in self.list_nodes(ex_datacenter=ex_datacenter):
            if node.extra['template']:
                image = NodeImage(
                    id=node.extra['uuid'],
                    name=node.name,
                    driver=self)
                images.append(image)

        return images

    def list_volumes(self, node=None):
        if node:
            vms = [self.ex_get_vm(node)]
        else:
            vms = self._list_vms()

        volumes = []
        for virtual_machine in vms:
            volumes.extend([
                self._to_volume(disk)
                for disk in self._get_vm_disks(virtual_machine)])

        return volumes

    def ex_get_vm(self, node_or_uuid):
        """
        Searches VMs for a given instance_uuid or Node object.

        :return pyVmomi.VmomiSupport.vim.VirtualMachine:
        """
        if isinstance(node_or_uuid, Node):
            node_or_uuid = node_or_uuid.extra['instance_uuid']
        vm = self.connection.client.content.searchIndex.FindByUuid(
            None, node_or_uuid, True, True)
        if not vm:
            raise LibcloudError("Unable to locate VirtualMachine.")
        return vm

    def list_nodes(self, ex_datacenter=None, ex_cluster=None,
                   ex_resource_pool=None):
        if ex_cluster is not None:
            raise NotImplemented('ex_cluster filter is not implemented yet.')
        if ex_resource_pool is not None:
            raise NotImplemented(
                'ex_resource_pool filter is not implemented yet.')

        vms = self._list_vms(datacenter=ex_datacenter,
                             cluster=ex_cluster,
                             resource_pool=ex_resource_pool)
        return [self._to_node(vm) for vm in vms]

    def _list_vms(self, datacenter=None, cluster=None, resource_pool=None):
        vms = []
        content = self.connection.client.RetrieveContent()
        for child in content.rootFolder.childEntity:
            if not hasattr(child, 'vmFolder'):
                continue
            if datacenter is not None and child.name != datacenter:
                continue

            vms.extend(self._get_vms(child.vmFolder))

        return vms

    def _get_vms(self, folder):
        vms = []
        for child in folder.childEntity:
            if hasattr(child, 'childEntity'):
                # if it's VM folder
                vms.extend(self._get_vms(child))
            else:
                # if it's a single VM
                vms.append(child)
        return vms

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

    def _get_vm_devices(self, virtual_machine):
        devices = {}
        for dev in virtual_machine.config.hardware.device:
            d = {
                'key': dev.key,
                'unit_number': getattr(dev, 'unitNumber', None),
                'label': getattr(getattr(dev, 'deviceInfo', None),
                                 'label', None),
                'summary': getattr(getattr(dev, 'deviceInfo', None),
                                   'summary', None),
            }
            # Network Device
            if hasattr(dev, 'macAddress'):
                d['mac_address'] = dev.macAddress
                d['address_type'] = getattr(dev, 'addressType', None)
            # Video Card
            if hasattr(dev, 'videoRamSizeInKB'):
                d['video_ram_size_in_kb'] = dev.videoRamSizeInKB
            # Disk
            if hasattr(dev, 'capacityInKB'):
                d['capacity_in_kb'] = dev.capacityInKB
            # Controller
            if hasattr(dev, 'busNumber'):
                d['bus_number'] = dev.busNumber
                d['devices'] = getattr(dev, 'device', [])

            devices[dev.key] = d
        return devices

    def _get_vm_disks(self, virtual_machine):
        if not virtual_machine.layoutEx:
            return []

        files = {}
        for file_info in virtual_machine.layoutEx.file:
            files[file_info.key] = {
                'accessible': file_info.accessible,
                'backing_object_id': file_info.backingObjectId,
                'name': file_info.name,
                'key': file_info.key,
                'size': file_info.size,
                'type': file_info.type,
                'unique_size': file_info.uniqueSize}

        vm_devices = self._get_vm_devices(virtual_machine)
        disks = []
        for disk_info in virtual_machine.layoutEx.disk:
            disk_files = []
            committed = 0
            descriptor = None
            for chain in getattr(disk_info, "chain", ()):
                for file_key in chain.fileKey:
                    f = files[file_key]
                    disk_files.append(f)
                    if f['type'] == 'diskExtent':
                        committed += f['size']
                    if f['type'] == 'diskDescriptor':
                        descriptor = f['name']

            device = vm_devices[disk_info.key]
            disks.append({
                'device': device,
                'files': disk_files,
                'capacity': device['capacity_in_kb'],
                'committed': int(committed / 1024),
                'descriptor': descriptor,
                'label': device['label'],
            })
        return disks

    def ex_get_node_by_uuid(self, uuid):
        return self._to_node(self.ex_get_vm(uuid))

    def _to_volume(self, disk):
        """
        Creates StorageVolume object from disk dictionary.

        :param dict disk: dict in format that _get_vm_disks() returns.

        :returns StorageVolume: Storage volume object
        """
        return StorageVolume(id=disk['device']['key'],
                             name=disk['label'],
                             size=int(disk['capacity']),
                             driver=self,
                             extra=disk)
