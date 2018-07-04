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

from __future__ import with_statement

import functools
import itertools
import uuid
from datetime import datetime

import mock

from libcloud.compute.providers import get_driver
from libcloud.compute.types import NodeState
from libcloud.compute.types import Provider
from libcloud.test import LibcloudTestCase


class PropertyCollectorMock(object):

    TraversalSpec = type('TraversalSpec', (mock.MagicMock,), {})
    ObjectSpec = type('ObjectSpec', (mock.MagicMock,), {})
    PropertySpec = type('PropertySpec', (mock.MagicMock,), {})
    FilterSpec = type('FilterSpec', (mock.MagicMock,), {})
    RetrieveOptions = type('RetrieveOptions', (mock.MagicMock,), {})


class VSphereConnectionMock(object):

    def __init__(self, *args, **kwargs):
        self.client = None

        self.history_collector = mock.MagicMock(
            latestPage=None
        )
        self.event_manager = mock.MagicMock(
            CreateCollectorForEvents=lambda *args: self.history_collector
        )

        self._property_collector_path_set = []
        self._property_collector_page_size = None
        self._property_collector_objects = []
        self.property_collector = mock.MagicMock(
            RetrievePropertiesEx=self._get_property_collector_page,
            ContinueRetrievePropertiesEx=self._get_property_collector_page
        )

        self.content = mock.MagicMock(
            eventManager=self.event_manager,
            propertyCollector=self.property_collector
        )

    def connect(self):
        self.client = mock.MagicMock(
            content=self.content,
            RetrieveContent=lambda *args: self.content
        )

    def destroy(self):
        self.client = None

    def put_to_property_collector(self, obj):
        self._property_collector_objects.append(obj)

    def _get_property_collector_page(self, *args, token=1):
        if args:
            (filter_spec,), options = args
            assert len(filter_spec.propSet) == 1
            assert len(filter_spec.objectSet) == 1
            self._property_collector_path_set = filter_spec.propSet[0].pathSet
            self._property_collector_page_size = options.maxObjects

        if self._property_collector_page_size:
            counter = range(self._property_collector_page_size)
        else:
            counter = itertools.count()

        objects = []
        for _ in counter:
            if not self._property_collector_objects:
                break

            obj = self._property_collector_objects.pop()
            prop_set = [
                create_mock(
                    name=key,
                    val=functools.reduce(getattr, key.split('.'), obj))
                for key in self._property_collector_path_set or []]
            objects.append(create_mock(obj=obj, propSet=prop_set))

        return mock.MagicMock(
            objects=objects,
            token=(token + 1) if self._property_collector_objects else None)


def create_mock(**kwargs):
    # https://docs.python.org/3/library/unittest.mock.html#mock-names-and-the-name-attribute
    obj = mock.Mock(**{
        key: value for key, value in kwargs.items()
        if key != 'name'})
    if 'name' in kwargs:
        obj.name = kwargs['name']
    return obj


@mock.patch('pyVmomi.vmodl.query.PropertyCollector', PropertyCollectorMock)
class VSphereNodeDriverests(LibcloudTestCase):

    USERNAME = 'test-username'
    PASSWORD = 'test-password'
    URL = 'https://test-vcenter.host'

    def setUp(self):
        VSphereNodeDriver = get_driver(Provider.VSPHERE)
        VSphereNodeDriver.connectionCls = VSphereConnectionMock
        self.driver = VSphereNodeDriver(
            self.USERNAME,
            self.PASSWORD,
            url=self.URL)

    def tearDown(self):
        self.driver = None

    def test_list_nodes(self):
        virtual_machines = [
            self._create_virtual_mashine(is_template=False)
            for _ in range(300)]
        virtual_machine_templates = [
            self._create_virtual_mashine(is_template=True)
            for _ in range(200)]
        virtual_machine_ids = [vm._GetMoId() for vm in virtual_machines]

        for vm in virtual_machines + virtual_machine_templates:
            self.driver.connection.put_to_property_collector(vm)

        nodes = self.driver.list_nodes()

        assert len(nodes) == 300
        assert all(node.id in virtual_machine_ids for node in nodes)

    def test_list_images(self):
        virtual_machines = [
            self._create_virtual_mashine(is_template=False)
            for _ in range(300)]
        virtual_machine_templates = [
            self._create_virtual_mashine(is_template=True)
            for _ in range(200)]
        virtual_machine_template_ids = [
            vm._GetMoId() for vm in virtual_machine_templates]

        for vm in virtual_machines + virtual_machine_templates:
            self.driver.connection.put_to_property_collector(vm)

        images = self.driver.list_images()

        assert len(images) == 200
        assert all(
            image.extra['managed_object_id'] in virtual_machine_template_ids
            for image in images)

    def test_list_volumes(self):
        pass

    def test_list_snapshots(self):
        pass

    def test_to_node(self):
        boot_time = datetime.now()

        self.driver.connection.put_to_property_collector(
            self._create_virtual_mashine(
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
                        ipAddress=None,
                    )
                )
            ))

        nodes = self.driver.list_nodes()

        assert len(nodes) == 1
        assert nodes[0].id == 'vm-1111'
        assert nodes[0].name == 'centos'
        assert nodes[0].state == NodeState.RUNNING
        assert nodes[0].public_ips == []
        assert nodes[0].private_ips == []
        assert nodes[0].extra == {
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
        }

    def _create_virtual_mashine(self, id=None, summary=None, is_template=None):
        vm_id = id or 'vm-{}'.format(str(uuid.uuid4())[0:8])
        summary = summary or create_mock(
            overallStatus=None,
            config=create_mock(
                name=None,
                template=False,
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
            ))
        if is_template is not None:
            summary.config.template = is_template
        return create_mock(_GetMoId=lambda: vm_id, summary=summary)
