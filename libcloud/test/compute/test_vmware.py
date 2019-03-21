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

# pylint: disable=protected-access
# pylint: disable=unused-argument
from __future__ import with_statement

import functools
import sys
import unittest
import uuid
from datetime import datetime

import pytest
import mock

from libcloud import test
from libcloud.common import types as common_types
from libcloud.compute import base as compute_types
from libcloud.compute import providers

sys.modules['pyVim'] = mock.Mock()
sys.modules['pyVmomi'] = mock.Mock()
from libcloud.compute.drivers import vmware


def create_mock(cls=mock.Mock, **kwargs):
    # pylint: disable=line-too-long
    # see: https://docs.python.org/3/library/unittest.mock.html#mock-names-and-the-name-attribute
    obj = cls(**{
        key: value for key, value in kwargs.items()
        if key != 'name'})
    if 'name' in kwargs:
        obj.name = kwargs['name']
    return obj


class PyVmomiTypes:

    VirtualMachine = type('VirtualMachine', (mock.MagicMock,), {})
    Datastore = type('Datastore', (mock.MagicMock,), {})
    Datacenter = type('Datacenter', (mock.MagicMock,), {})
    VirtualDisk = type('VirtualDisk', (mock.MagicMock,), {})
    VirtualSCSIController = type('VirtualSCSIController', (mock.MagicMock,), {})
    FileQuery = type('FileQuery', (mock.MagicMock,), {})
    DatastoreUrlPair = type('DatastoreUrlPair', (mock.MagicMock,), {})


class PropertyCollectorMock(object):
    """
    Implements PropertyCollector-like iteration.
    """

    _objects = tuple()  # type: tuple

    def __init__(self, driver, type, path_set=None, process_fn=None, **kwargs):
        self._path_set = path_set
        self._process_fn = process_fn

    @classmethod
    def set_objects(cls, *args):
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            cls._objects = tuple(args[0])
        else:
            cls._objects = args

    def __iter__(self):
        if self._path_set is None:
            result = self._objects
        else:
            result = {
                obj: {
                    key: functools.reduce(getattr, key.split('.'), obj)
                    for key in self._path_set
                } for obj in self._objects}
        result = self._process_fn(result) if self._process_fn else result
        return iter(result)


class VSpherePropertyCollectorTests(test.LibcloudTestCase):

    def setUp(self):
        self.property_collector = mock.MagicMock()
        self.content = mock.Mock(propertyCollector=self.property_collector)
        self.driver = mock.Mock(connection=mock.Mock(content=self.content))

    def tearDown(self):
        self.property_collector = None
        self.content = None
        self.driver = None

    @mock.patch('pyVmomi.vmodl.query.PropertyCollector.TraversalSpec')
    @mock.patch('pyVmomi.vmodl.query.PropertyCollector.ObjectSpec')
    @mock.patch('pyVmomi.vmodl.query.PropertyCollector.PropertySpec')
    @mock.patch('pyVmomi.vmodl.query.PropertyCollector.FilterSpec')
    @mock.patch('pyVmomi.vmodl.query.PropertyCollector.RetrieveOptions')
    def test_iter(self, *args):

        process_fn_calls = []
        def process_fn(result):
            process_fn_calls.append(result)
            return list(result.keys())

        self.property_collector.RetrievePropertiesEx.return_value = mock.Mock(
            objects=[
                mock.Mock(obj=1, propSet=[
                    create_mock(name='config', val='dev-1')
                ]),
                mock.Mock(obj=2, propSet=[
                    create_mock(name='config', val='dev-2')
                ])
            ],
            token=1
        )
        self.property_collector.ContinueRetrievePropertiesEx.side_effect = [
            mock.Mock(
                objects=[
                    mock.Mock(obj=3, propSet=[
                        create_mock(name='config', val='dev-3')
                    ]),
                    mock.Mock(obj=4, propSet=[
                        create_mock(name='config', val='dev-4')
                    ])
                ],
                token=2
            ),
            mock.Mock(
                objects=[
                    mock.Mock(obj=5, propSet=[
                        create_mock(name='config', val='dev-5')
                    ]),
                ],
                token=None
            ),
        ]

        objects = list(vmware.VSpherePropertyCollector(
            self.driver, mock.Mock(__name__='ManagedEntity'),
            path_set=['config'],
            process_fn=process_fn,
        ))

        self.assertEqual(set(objects), {1, 2, 3, 4, 5})
        self.assertEqual(process_fn_calls, [
            {1: {'config': 'dev-1'}, 2: {'config': 'dev-2'}},
            {3: {'config': 'dev-3'}, 4: {'config': 'dev-4'}},
            {5: {'config': 'dev-5'}},
        ])


class VSphereConnectionTests(test.LibcloudTestCase):
    """
    Tests for VMWare VSphere connection.
    """

    def test_init_errors(self):
        with self.assertRaises(ValueError):
            vmware.VSphereConnection('test-user', 'test-password')
        with self.assertRaises(ValueError):
            vmware.VSphereConnection(
                'test-user', 'test-password',
                url='https://test-vcenter.host',
                host='test-vcenter.host')

    @mock.patch.object(vmware, 'connect')
    @mock.patch.object(vmware.ssl, '_create_unverified_context')
    def test_connect(self, create_context_fn, connect_fn):
        connection = vmware.VSphereConnection(
            'test-user',
            'test-password',
            url='https://test-vcenter.host')
        connection.connect()

        connect_fn.SmartConnect.assert_called_once_with(
            protocol='https',
            host='test-vcenter.host',
            port=443,
            user='test-user',
            pwd='test-password',
            path='/sdk',
            sslContext=create_context_fn.return_value)

    @mock.patch.object(vmware, 'connect')
    def test_connect_with_invalid_username(self, connect_fn):
        conn = vmware.VSphereConnection('user', 'pass', url='http://url.com')
        connect_fn.SmartConnect.side_effect = Exception('incorrect user name')
        with self.assertRaises(common_types.InvalidCredsError) as ctx:
            conn.connect()
        self.assertEqual(
            ctx.exception.value,
            "Check that your username and password are valid.")

    @mock.patch.object(vmware, 'connect')
    def test_connection_refused(self, connect_fn):
        conn = vmware.VSphereConnection('user', 'pass', url='http://url.com')
        connect_fn.SmartConnect.side_effect = Exception('connection refused')

        with self.assertRaises(common_types.LibcloudError) as ctx:
            conn.connect()
        err_msg = \
            "Check that the host provided (http://url.com:80/sdk) is a " \
            "vSphere installation."
        self.assertEqual(ctx.exception.value, err_msg)

    @mock.patch.object(vmware, 'connect')
    def test_connect_to_unknown_host(self, connect_fn):
        conn = vmware.VSphereConnection('user', 'pass', url='http://url.com')
        connect_fn.SmartConnect.side_effect = Exception(
            'name or service not known')

        with self.assertRaises(common_types.LibcloudError) as ctx:
            conn.connect()
        self.assertEqual(
            ctx.exception.value,
            "Check that the vSphere (http://url.com:80/sdk) is accessible.")


@mock.patch(
    'pyVmomi.vim.vm.device.VirtualSCSIController', 
    PyVmomiTypes.VirtualSCSIController)
@mock.patch(
    'pyVmomi.vim.vm.device.VirtualDisk',
    PyVmomiTypes.VirtualDisk)
@mock.patch(
    'pyVmomi.vim.vm.ConfigInfo.DatastoreUrlPair',
    PyVmomiTypes.DatastoreUrlPair)
@mock.patch('pyVmomi.vim.FileQuery', PyVmomiTypes.FileQuery)
@mock.patch('pyVmomi.vim.VmDiskFileQuery', PyVmomiTypes.FileQuery)
@mock.patch.object(vmware, 'VSpherePropertyCollector', PropertyCollectorMock)
class VSphereNodeDriverTests(test.LibcloudTestCase):
    """
    Tests for VMWare VSphere node driver.
    """

    USERNAME = 'test-username'
    PASSWORD = 'test-password'
    URL = 'https://test-vcenter.host'

    def setUp(self):
        VSphereNodeDriver = providers.get_driver(providers.Provider.VSPHERE)
        VSphereNodeDriver.connectionCls = create_mock()
        self.driver = VSphereNodeDriver(
            self.USERNAME, self.PASSWORD,
            url=self.URL)

    def tearDown(self):
        self.driver = None

    ###########################################################################
    #  Tests for list methods                                                 #
    ###########################################################################

    def test_list_nodes(self):
        virtual_machines = [
            self.create_virtual_machine(is_template=False)
            for _ in range(300)]
        vm_templates = [
            self.create_virtual_machine(is_template=True)
            for _ in range(200)]
        virtual_machine_ids = [vm._GetMoId() for vm in virtual_machines]

        self.driver._query_node_creation_times = create_mock()
        PropertyCollectorMock.set_objects(virtual_machines + vm_templates)

        nodes = self.driver.list_nodes()

        self.assertEqual(len(nodes), 300)
        self.assertTrue(all(node.id in virtual_machine_ids for node in nodes))

    def test_list_images(self):
        virtual_machines = [
            self.create_virtual_machine(is_template=False)
            for _ in range(300)]
        vm_templates = [
            self.create_virtual_machine(is_template=True)
            for _ in range(200)]
        vm_templates_ids = [vm._GetMoId() for vm in vm_templates]

        self.driver._query_node_creation_times = create_mock()
        PropertyCollectorMock.set_objects(virtual_machines + vm_templates)

        images = self.driver.list_images()

        self.assertEqual(len(images), 200)
        self.assertTrue(all(
            image.extra['managed_object_id'] in vm_templates_ids
            for image in images
        ))

    @pytest.mark.skip
    def test_list_volumes(self):
        self.driver._query_vm_virtual_disks = lambda **kwargs: {
            vmware._VMDiskInfo(
                disk_id='100-100',
                owner_id='vm-1111',
                file_path='ds:///vmfs/volumes/1/dir-1/centos7.vmdk',
                sharing=True,
            ),
            vmware._VMDiskInfo(
                disk_id='100-200',
                owner_id='vm-2222',
                file_path='ds:///vmfs/volumes/1/dir-1/centos7.vmdk',
                sharing=True,
            ),
            vmware._VMDiskInfo(
                disk_id='100-300',
                owner_id='vm-3333',
                file_path='ds:///vmfs/volumes/1/dir-2/debian8.vmdk',
                scsi_host=7,
                scsi_bus=0,
                scsi_target=0,
                scsi_lun=0,
            )}
        self.driver._query_node_creation_times = lambda **kwargs: {
            'vm-3333': 'time-3'}
        self.driver._query_volume_creation_times = lambda **kwargs: {
            'ds:///vmfs/volumes/1/dir-1/centos7.vmdk': 'time-1',
            'ds:///vmfs/volumes/1/dir-1/ubuntu1404.vmdk': 'time-2'}

        files_ds1 = [
            create_mock(
                folderPath='[dc1] dir-1/',
                file=[
                    create_mock(
                        path='centos7.vmdk',
                        fileSize=700 * (1024 ** 3)
                    ),
                    create_mock(
                        path='ubuntu1404.vmdk',
                        fileSize=800 * (1024 ** 3)
                    )],),
            create_mock(
                folderPath='[dc1] dir-2/',
                file=[
                    create_mock(
                        path='debian8.vmdk',
                        fileSize=400 * (1024 ** 3),
                    )
                ]
            )]
        files_ds2 = [
            create_mock(
                folderPath='[dc2] dir-1/',
                file=[
                    create_mock(
                        path='debian9.vmdk',
                        fileSize=410 * (1024 ** 3),
                    )
                ]
            )]

        PropertyCollectorMock.set_objects(
            self.create_datastore(
                info=create_mock(name='dc1', url='ds:///vmfs/volumes/1/'),
                browser=create_mock(
                    SearchSubFolders=lambda *args: create_mock(
                        info=create_mock(
                            state=vmware.vim.TaskInfo.State.success,
                            result=files_ds1)
                    ))
            ),
            self.create_datastore(
                info=create_mock(name='dc3', url='ds:///vmfs/volumes/3/'),
                browser=create_mock(
                    SearchSubFolders=lambda *args: create_mock(
                        info=create_mock(
                            state=vmware.vim.TaskInfo.State.error,
                            result=[])
                    ))
            ),
            self.create_datastore(
                info=create_mock(name='dc2', url='ds:///vmfs/volumes/2/'),
                browser=create_mock(
                    SearchSubFolders=lambda *args: create_mock(
                        info=create_mock(
                            state=vmware.vim.TaskInfo.State.success,
                            result=files_ds2)
                    ))
            ),)

        volumes = self.driver.list_volumes()
        self.assertEqual(len(volumes), 4)
        self.assertEqual({volume.id for volume in volumes}, {
            'ds:///vmfs/volumes/1/dir-1/centos7.vmdk',
            'ds:///vmfs/volumes/1/dir-1/ubuntu1404.vmdk',
            'ds:///vmfs/volumes/1/dir-2/debian8.vmdk',
            'ds:///vmfs/volumes/2/dir-1/debian9.vmdk'
        })

        volumes = {v.id.split('/')[-1].split('.')[0]: v for v in volumes}
        self.assertEqual(volumes['centos7'].size, 700)
        self.assertEqual(volumes['ubuntu1404'].size, 800)
        self.assertEqual(volumes['debian8'].size, 400)
        self.assertEqual(volumes['debian9'].size, 410)

        self.assertEqual(set(volumes['centos7'].extra['devices'].keys()), {
            'vm-1111', 'vm-2222'})
        self.assertDictEqual(volumes['ubuntu1404'].extra['devices'], {})
        self.assertEqual(set(volumes['debian8'].extra['devices'].keys()), {
            'vm-3333'})
        self.assertDictEqual(volumes['debian9'].extra['devices'], {})

        self.assertEqual(volumes['centos7'].extra['created_at'], 'time-1')
        self.assertEqual(volumes['ubuntu1404'].extra['created_at'], 'time-2')
        self.assertEqual(volumes['debian8'].extra['created_at'], 'time-3')
        self.assertIsNone(volumes['debian9'].extra['created_at'])

    @pytest.mark.skip
    def test_list_volumes_on_node(self):
        vm = self.create_virtual_machine(id='vm-1111')

        self.driver.ex_get_vm = create_mock(return_value=vm)
        self.driver._query_node_creation_times = lambda **kwargs: {}
        self.driver._query_volume_creation_times = lambda **kwargs: {}
        self.driver._query_vm_virtual_disks = lambda **kwargs: {
            vmware._VMDiskInfo(
                disk_id='100-100',
                owner_id='vm-1111',
                file_path='ds:///vmfs/volumes/1/dir-1/centos7.vmdk',
            ),
            vmware._VMDiskInfo(
                disk_id='100-100',
                owner_id='vm-1111',
                file_path='ds:///vmfs/volumes/1/dir-1/centos7_1.vmdk',
            ),
        }

        volumes = self.driver.list_volumes(node='vm-1111')

        self.assertEqual(len(volumes), 2)
        self.driver.ex_get_vm.assert_called_once_with('vm-1111')

    def test_list_snapshots(self):
        vm_1 = self.create_virtual_machine(
            id='vm-1111',
            snapshot=create_mock(rootSnapshotList=None))
        vm_2 = self.create_virtual_machine(id='vm-2222')
        vm_2.snapshot = create_mock(
            rootSnapshotList=[
                create_mock(
                    vm=vm_2,
                    id=1,
                    snapshot=1,
                    name='snap-1',
                    createTime='time-1',
                    childSnapshotList=[
                        create_mock(
                            vm=vm_2,
                            id=2,
                            snapshot=2,
                            name='snap-2',
                            createTime='time-2',
                            childSnapshotList=[],
                        ),
                        create_mock(
                            vm=vm_2,
                            id=3,
                            snapshot=3,
                            name='snap-3',
                            createTime='time-3',
                            childSnapshotList=[],
                        )
                    ],
                )
            ]
        )
        vm_2.layoutEx = create_mock(
            snapshot=[
                create_mock(dataKey=1, key=1),
                create_mock(dataKey=1, key=1),
                create_mock(dataKey=2, key=2),
                create_mock(dataKey=2, key=2),
                create_mock(dataKey=3, key=3),
                create_mock(dataKey=3, key=3),
            ],
            file=[
                create_mock(size=10 * (1024 ** 3), key=1, type='snapshotData'),
                create_mock(size=11 * (1024 ** 3), key=1, type='unknownData'),
                create_mock(size=12 * (1024 ** 3), key=2, type='snapshotData'),
                create_mock(size=13 * (1024 ** 3), key=2, type='unknownData'),
                create_mock(size=14 * (1024 ** 3), key=3, type='snapshotData'),
                create_mock(size=15 * (1024 ** 3), key=3, type='snapshotData'),
            ]
        )

        PropertyCollectorMock.set_objects(vm_1, vm_2)

        snapshot = self.driver.list_snapshots()

        self.assertEqual(len(snapshot), 3)
        self.assertEqual(snapshot[0].id, 1)
        self.assertEqual(snapshot[0].name, 'snap-1')
        self.assertEqual(snapshot[0].created, 'time-1')
        self.assertEqual(snapshot[0].size, 10)

        self.assertEqual(snapshot[1].id, 2)
        self.assertEqual(snapshot[1].name, 'snap-2')
        self.assertEqual(snapshot[1].created, 'time-2')
        self.assertEqual(snapshot[1].size, 12)

        self.assertEqual(snapshot[2].id, 3)
        self.assertEqual(snapshot[2].name, 'snap-3')
        self.assertEqual(snapshot[2].created, 'time-3')
        self.assertEqual(snapshot[2].size, 29)

    ###########################################################################
    #  Tests for extra methods                                                #
    ###########################################################################

    def test_ex_get_vm_by_uuid(self):
        vm = self.driver.ex_get_vm('vm-1111')

        content = self.driver.connection.content
        content.searchIndex.FindByUuid.assert_called_once_with(
            None, 'vm-1111', True, True)
        self.assertEqual(vm, content.searchIndex.FindByUuid.return_value)

    def test_ex_get_vm_by_node(self):
        node = compute_types.Node(None, None, None, None, None, None, extra={
            'instance_uuid': 'vm-2222'
        })
        vm = self.driver.ex_get_vm(node)

        content = self.driver.connection.content
        content.searchIndex.FindByUuid.assert_called_once_with(
            None, 'vm-2222', True, True)
        self.assertEqual(vm, content.searchIndex.FindByUuid.return_value)

    def test_ex_list_datacenters(self):
        PropertyCollectorMock.set_objects(
            self.create_datacenter(),
            self.create_datacenter(),
            self.create_datacenter())

        self.assertEqual(len(self.driver.ex_list_datacenters()), 3)

    ###########################################################################
    #  Tests for private methods                                              #
    ###########################################################################

    def test__to_node(self):
        boot_time = datetime(2017, 7, 28, 0, 26, 10, 0)
        create_time = datetime(2017, 7, 28, 0, 27, 10, 0)
        vm = self.create_virtual_machine(
            id='vm-1111',
            summary=create_mock(
                overallStatus='overall-status',
                config=create_mock(
                    name='centos',
                    template=False,
                    uuid='uuid',
                    instanceUuid='instance-uuid',
                    vmPathName='vm-path-name',
                    guestId='guest-id',
                    guestFullName='os-name',
                    numCpu=10,
                    memorySizeMB=1200,
                    annotation='annotation-text',
                ),
                runtime=create_mock(
                    powerState='poweredOn',
                    bootTime=boot_time,
                ),
                guest=create_mock(
                    ipAddress='192.168.0.1',
                ),
                customValue=[mock.Mock(key='custom-key', value='custom-value')]
            ))
        PropertyCollectorMock.set_objects(vm)
        self.driver._query_node_creation_times = create_mock(return_value={
            'vm-1111': create_time
        })

        nodes = self.driver.list_nodes()

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].id, 'vm-1111')
        self.assertEqual(nodes[0].name, 'centos')
        self.assertEqual(nodes[0].state, compute_types.NodeState.RUNNING)
        self.assertEqual(nodes[0].public_ips, [])
        self.assertEqual(nodes[0].private_ips, ['192.168.0.1'])
        self.assertEqual(nodes[0].extra, {
            'uuid': 'uuid',
            'instance_uuid': 'instance-uuid',
            'path': 'vm-path-name',
            'guest_id': 'guest-id',
            'template': False,
            'overall_status': 'overall-status',
            'operating_system': 'os-name',
            'cpus': 10,
            'memory_mb': 1200,
            'boot_time': boot_time.isoformat(),
            'annotation': 'annotation-text',
            'datacenter': None,
            'custom_value': {'custom-key': 'custom-value'},
        })
        self.assertEqual(nodes[0].created_at, create_time)

    def test__to_image(self):
        PropertyCollectorMock.set_objects(
            self.create_virtual_machine(
                id='vm-1111',
                summary=create_mock(config=create_mock(
                    template=True,
                    uuid='vm-uuid',
                )))
        )
        self.driver._query_node_creation_times = create_mock(return_value={
            'vm-1111': 'time-1'
        })

        images = self.driver.list_images()

        self.assertEqual(len(images), 1)
        self.assertEqual(images[0].id, 'vm-uuid')
        self.assertEqual(images[0].extra, {
            'managed_object_id': 'vm-1111',
            'datacenter': None,
        })
        self.assertEqual(images[0].created_at, 'time-1')

    def test__query_vm_virtual_disks(self):
        PropertyCollectorMock.set_objects(
            self.create_datacenter(datastore=[
                create_mock(info=create_mock(
                    name='ds.dc1',
                    url='ds:///vmfs/volumes/1/',
                ))
            ]),
            self.create_virtual_machine(
                id='vm-1234',
                config=create_mock(
                    hardware=create_mock(
                        device=[
                            self.create_disk(
                                diskObjectId='100-200',
                                key=213,
                                controllerKey=3,
                                unitNumber=4,
                                backing=create_mock(
                                    fileName='[ds.dc1] centos.vmdk'
                                ),
                            ),
                            self.create_disk(
                                key=214,
                                backing=None,
                            ),
                            self.create_scsi_controller(
                                device=[213, 214],
                                scsiCtlrUnitNumber=1,
                                busNumber=2,
                            )]
                        )
                    )
                )
        )

        disks = list(self.driver._query_vm_virtual_disks())

        self.assertEqual(len(disks), 1)
        self.assertTrue(isinstance(disks[0], vmware._VMDiskInfo))
        self.assertEqual(disks[0].disk_id, '100-200')
        self.assertEqual(disks[0].owner_id, 'vm-1234')
        self.assertEqual(disks[0].file_path, 'ds:///vmfs/volumes/1/centos.vmdk')
        self.assertEqual(disks[0].scsi_host, 1)
        self.assertEqual(disks[0].scsi_bus, 2)
        self.assertEqual(disks[0].scsi_target, 3)
        self.assertEqual(disks[0].scsi_lun, 4)

    def test__query_node_creation_times(self):
        vm_1 = self.create_virtual_machine()
        vm_2 = self.create_virtual_machine()
        vm_3 = self.create_virtual_machine()

        self.driver._query_events = create_mock(return_value=[
            create_mock(
                vm=create_mock(vm=vm_1),
                createdTime='timestamp-1'
            ),
            create_mock(
                vm=create_mock(vm=vm_2),
                createdTime='timestamp-2'
            ),
            create_mock(
                vm=create_mock(vm=vm_3),
                createdTime='timestamp-3'
            )
        ])

        self.assertDictEqual(self.driver._query_node_creation_times(), {
            vm_1._GetMoId(): 'timestamp-1',
            vm_2._GetMoId(): 'timestamp-2',
            vm_3._GetMoId(): 'timestamp-3',
        })

    def test__query_volume_creation_times(self):
        self.driver._query_events = create_mock(return_value=[
            create_mock(
                configSpec=create_mock(deviceChange=[
                    create_mock(
                        device=self.create_disk(
                            backing=create_mock(fileName='disk-1')
                        ),
                        operation='add',
                        fileOperation='create',
                    )
                ]),
                createdTime='timestamp-1'
            ),
            create_mock(
                configSpec=create_mock(deviceChange=[
                    create_mock(
                        device=self.create_disk(
                            backing=create_mock(fileName='disk-2')
                        ),
                        operation='add',
                        fileOperation='replace',
                    )
                ]),
                createdTime='timestamp-2'),
            create_mock(
                configSpec=create_mock(deviceChange=[
                    create_mock(
                        device=self.create_disk(
                            backing=create_mock(fileName='disk-3')
                        ),
                        operation='add',
                        fileOperation='create',
                    )
                ]),
                createdTime='timestamp-3')
        ])

        self.assertDictEqual(self.driver._query_volume_creation_times(), {
            'disk-1': 'timestamp-1',
            'disk-3': 'timestamp-3',
        })

    @mock.patch('pyVmomi.vim.event.EventFilterSpec')
    def test__query_events(self, *args):
        history_collector = create_mock()
        event_manager = mock.Mock(
            CreateCollectorForEvents=mock.Mock(return_value=history_collector))
        self.driver.connection.content.eventManager = event_manager

        history_collector.latestPage = [1, 2]
        history_collector.ReadPreviousEvents.side_effect = [[3, 4], [5], []]
        events = list(self.driver._query_events(page_size=200))

        self.assertEqual(events, [1, 2, 3, 4, 5])
        event_manager.CreateCollectorForEvents.assert_called_once()
        history_collector.SetCollectorPageSize.assert_called_once_with(200)
        history_collector.ResetCollector.assert_called_once()

    ###########################################################################
    #  Helpers                                                                #
    ###########################################################################

    def create_virtual_machine(self, id=None, summary=None, **kwargs):
        vm_id = id or 'vm-{}'.format(str(uuid.uuid4())[0:8])
        summary = summary or create_mock(
            overallStatus=None,
            config=create_mock(
                name=None,
                template=kwargs.get('is_template'),
                uuid=None,
                instanceUuid=None,
                vmPathName=None,
                guestId=None,
                guestFullName=None,
                numCpu=None,
                memorySizeMB=None,
                annotation=None,
            ),
            runtime=create_mock(
                powerState=None,
                bootTime=None,
            ),
            guest=create_mock(
                ipAddress=None,
            ),
            customValue=[])
        return create_mock(
            cls=PyVmomiTypes.VirtualMachine,
            _GetMoId=lambda: vm_id,
            _moId=vm_id,
            summary=summary,
            **kwargs)

    def create_datacenter(self, **kwargs):
        return create_mock(cls=PyVmomiTypes.Datacenter, **kwargs)

    def create_datastore(self, **kwargs):
        return create_mock(cls=PyVmomiTypes.Datastore, **kwargs)

    def create_disk(self, **kwargs):
        return create_mock(cls=PyVmomiTypes.VirtualDisk, **kwargs)

    def create_scsi_controller(self, **kwargs):
        return create_mock(
            cls=PyVmomiTypes.VirtualSCSIController,
            **kwargs)


if __name__ == '__main__':
    sys.exit(unittest.main())
