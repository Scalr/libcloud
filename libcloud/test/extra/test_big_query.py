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
Tests for Google Connection classes.
"""
import datetime
import mock
import os
import pytz
import sys
import unittest

from libcloud.common.google import GoogleAuthType
from libcloud.extra.drivers.google import BQConnection, BigQuery
from libcloud.test import MockHttp, LibcloudTestCase
from libcloud.test.file_fixtures import ExtraFileFixtures
from libcloud.utils.py3 import httplib


SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
PEM_KEY = os.path.join(SCRIPT_PATH, 'fixtures', 'big_query', 'pkey.pem')
GCE_PARAMS_PEM_KEY = ('email@developer.gserviceaccount.com', PEM_KEY)
STUB_TOKEN_FROM_FILE = {
    'access_token': 'token_from_file',
    'token_type': 'Bearer',
    'expire_time': (datetime.datetime.utcnow() + datetime.timedelta(seconds=3600)).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'expires_in': 3600
}


class BigQueryMockHttp(MockHttp):
    fixtures = ExtraFileFixtures('big_query')

    def _datasets(self, method, url, body, headers):
        body = self.fixtures.load('list_datasets.json')
        return (httplib.OK, body, {}, httplib.responses[httplib.OK])

    def _bigquery_v2_projects_my_cool_project_datasets(self, method, url, body, headers):
        body = self.fixtures.load('list_datasets.json')
        return (httplib.OK, body, {}, httplib.responses[httplib.OK])

    def _api_dns_list_domain_asp(self, method, url, body, headers):
        body = self.fixtures.load('api_dns_list_domain_asp.json')
        return (httplib.OK, body, {}, httplib.responses[httplib.OK])

    def _bigquery_v2_projects_my_cool_project_datasets_billing_tables(self, method, url, body, headers):
        body = self.fixtures.load('list_tables.json')
        return (httplib.OK, body, {}, httplib.responses[httplib.OK])

    def _bigquery_v2_projects_my_cool_project_queries(self, method, url, body, headers):
        body = self.fixtures.load('query.json')
        return (httplib.OK, body, {}, httplib.responses[httplib.OK])

    def _bigquery_v2_projects_my_cool_project_queries_job_tRcSDn7P4_O_rdNjX42n0d67VFFB(self, method, url, body, headers):
        body = self.fixtures.load('query_results.json')
        return (httplib.OK, body, {}, httplib.responses[httplib.OK])


class GoogleTestCase(LibcloudTestCase):
    """
    Assists in making Google tests hermetic and deterministic.

    Add anything that needs to be mocked here. Create a patcher with the
    suffix '_patcher'.

    e.g.
        _foo_patcher = mock.patch('module.submodule.class.foo', ...)

    Patchers are started at setUpClass and stopped at tearDownClass.

    Ideally, you should make a note in the thing being mocked, for clarity.
    """
    PATCHER_SUFFIX = '_patcher'

    _read_token_file_patcher = mock.patch(
        'libcloud.common.google.GoogleOAuth2Credential._get_token_from_file',
        return_value=STUB_TOKEN_FROM_FILE
    )

    @classmethod
    def setUpClass(cls):
        super(GoogleTestCase, cls).setUpClass()

        for patcher in [a for a in dir(cls) if a.endswith(cls.PATCHER_SUFFIX)]:
            getattr(cls, patcher).start()

    @classmethod
    def tearDownClass(cls):
        super(GoogleTestCase, cls).tearDownClass()

        for patcher in [a for a in dir(cls) if a.endswith(cls.PATCHER_SUFFIX)]:
            getattr(cls, patcher).stop()


class BQConnectionTest(GoogleTestCase):
    """
    Tests for GoogleBaseAuthConnection
    """

    @mock.patch('libcloud.common.google.GoogleServiceAcctAuthConnection', mock.Mock())
    def setUp(self):
        self.project_id = "my_cool_project"
        self.conn = BQConnection(*GCE_PARAMS_PEM_KEY, project=self.project_id)

    def test_scopes(self):
        expected_requst_path = '/bigquery/v2/projects/%s' % self.project_id
        self.assertEqual(self.conn.request_path, expected_requst_path)


class BigQueryTest(GoogleTestCase):
    """
    Tests for GoogleInstalledAppAuthConnection
    """

    @mock.patch('libcloud.common.google.GoogleServiceAcctAuthConnection', mock.Mock())
    def setUp(self):
        self.project_id = "my_cool_project"
        BigQuery.connectionCls.conn_class = BigQueryMockHttp
        BigQueryMockHttp.type = None
        self.conn = BigQuery(*GCE_PARAMS_PEM_KEY, project=self.project_id)

    def test_init(self):
        expected_requst_path = '/bigquery/v2/projects/%s' % self.project_id
        self.assertEqual(self.conn.connection.oauth2_credential.scopes, BigQuery.default_scopes)
        self.assertEqual(self.conn.connection.oauth2_credential.auth_type, GoogleAuthType.SA)
        self.assertEqual(self.conn.connection.request_path, expected_requst_path)

    def test_list_datasets(self):
        expected_result = [{'datasetId': 'billing', 'projectId': 'scalr-labs'}]
        actual_result = self.conn.list_datasets()
        self.assertEqual(expected_result, actual_result)

    def test_list_tables(self):
        expected_result = [{'datasetId': 'billing',
                            'projectId': 'scalr-labs',
                            'tableId': 'gcp_billing_export_00D54C_776AB2_C6075D'},
                           {'datasetId': 'billing',
                            'projectId': 'scalr-labs',
                            'tableId': 'gcp_billing_export_v1_00D54C_776AB2_C6075D'}]
        actual_result = self.conn.list_tables('billing')

        self.assertEqual(expected_result, actual_result)

    def test_query(self):
        expected_result = [{'billing_account_id': '00D54C-776AB2-C6075D',
                            'cost': 0.001715,
                            'credits': [],
                            'dtime': '2017-11-26 08',
                            'export_time': datetime.datetime(2018, 1, 11, 23, 24, 17, 135000, tzinfo=pytz.utc),
                            'project_name': 'scalr-demo',
                            'service': 'Compute Engine',
                            'sku': 'Storage Image',
                            'usage_end_time': datetime.datetime(2017, 11, 26, 8, 25, 8, 389000, tzinfo=pytz.utc),
                            'usage_start_time': datetime.datetime(2017, 11, 26, 8, 0, tzinfo=pytz.utc)},
                           {'billing_account_id': '00D54C-776AB2-C6075D',
                            'cost': 0.006212,
                            'credits': [],
                            'dtime': '2017-11-26 08',
                            'export_time': datetime.datetime(2018, 1, 11, 23, 24, 17, 135000, tzinfo=pytz.utc),
                            'project_name': 'scalr-production',
                            'service': 'Compute Engine',
                            'sku': 'Storage Image',
                            'usage_end_time': datetime.datetime(2017, 11, 26, 8, 25, 8, 389000, tzinfo=pytz.utc),
                            'usage_start_time': datetime.datetime(2017, 11, 26, 8, 0, tzinfo=pytz.utc)},
                           {'billing_account_id': '00D54C-776AB2-C6075D',
                            'cost': 0.005993,
                            'credits': [],
                            'dtime': '2017-11-26 08',
                            'export_time': datetime.datetime(2018, 1, 11, 23, 24, 17, 135000, tzinfo=pytz.utc),
                            'project_name': 'scalr-development',
                            'service': 'Compute Engine',
                            'sku': 'Storage Image',
                            'usage_end_time': datetime.datetime(2017, 11, 26, 8, 25, 8, 389000, tzinfo=pytz.utc),
                            'usage_start_time': datetime.datetime(2017, 11, 26, 8, 0, tzinfo=pytz.utc)},
                           {'billing_account_id': '00D54C-776AB2-C6075D',
                            'cost': 0.008376,
                            'credits': [],
                            'dtime': '2017-11-26 08',
                            'export_time': datetime.datetime(2018, 1, 11, 23, 24, 17, 135000, tzinfo=pytz.utc),
                            'project_name': 'scalr-production',
                            'service': 'Compute Engine',
                            'sku': 'Static Ip Charge',
                            'usage_end_time': datetime.datetime(2017, 11, 26, 8, 25, 8, 389000, tzinfo=pytz.utc),
                            'usage_start_time': datetime.datetime(2017, 11, 26, 8, 0, tzinfo=pytz.utc)}]
        actual_result = list(self.conn.query('Some SQL expression'))
        self.assertEqual(expected_result, actual_result)


if __name__ == '__main__':
    sys.exit(unittest.main())
