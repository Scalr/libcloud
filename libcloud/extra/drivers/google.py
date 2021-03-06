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
Module for Google Big Data Drivers.
"""
from libcloud.extra.drivers.google_bq_utils import QueryJob
from libcloud.common.google import GoogleAuthType, GoogleBaseConnection
from libcloud.common.base import BaseDriver

API_VERSION = 'v2'


class BQConnection(GoogleBaseConnection):
    """
    Connection class for the BQ driver.
    """

    def __init__(self, user_id, key, secure=None, auth_type=None, credential_file=None, **kwargs):

        project = kwargs.pop('project')

        super(BQConnection, self).__init__(user_id, key, secure=secure, auth_type=auth_type,
                                           credential_file=credential_file, **kwargs)
        self.request_path = '/bigquery/%s/projects/%s' % (API_VERSION, project)


class BigQuery(BaseDriver):
    """ Google Big Query client """

    connectionCls = BQConnection
    api_name = 'google'
    name = 'Big Query'
    default_scopes = ['https://www.googleapis.com/auth/bigquery',
                      'https://www.googleapis.com/auth/bigquery.insertdata',
                      'https://www.googleapis.com/auth/cloud-platform.read-only',
                      'https://www.googleapis.com/auth/devstorage.full_control',
                      'https://www.googleapis.com/auth/devstorage.read_only',
                      'https://www.googleapis.com/auth/devstorage.read_write']

    def __init__(self, user_id, key, project, **kwargs):
        """
        :param  user_id: The email address (for service accounts) or Client ID
                         (for installed apps) to be used for authentication.
        :type   user_id: ``str``

        :param  key: The RSA Key (for service accounts) or file path containing
                     key or Client Secret (for installed apps) to be used for
                     authentication.
        :type   key: ``str``

        :keyword  project: Your  project name. (required)
        :type     project: ``str``

        :keyword  auth_type: Accepted values are "SA" or "IA" or "GCE"
                             ("Service Account" or "Installed Application" or
                             "GCE" if libcloud is being used on a GCE instance
                             with service account enabled).
                             If not supplied, auth_type will be guessed based
                             on value of user_id or if the code is being
                             executed in a GCE instance.
        :type     auth_type: ``str``

        :keyword  scopes: List of authorization URLs. Default is empty and
                          grants read/write to Compute, Storage, DNS.
        :type     scopes: ``list``
        """
        self.project = project
        if 'auth_type' not in kwargs:
            kwargs['auth_type'] = GoogleAuthType.SA

        self.scopes = kwargs.get('scopes', self.default_scopes)
        super(BigQuery, self).__init__(user_id, key, **kwargs)

    def _ex_connection_class_kwargs(self):
        """
        Add extra parameters to auth request
        """
        res = super(BigQuery, self)._ex_connection_class_kwargs()
        res['project'] = self.project
        res['scopes'] = self.scopes
        return res

    def list_datasets(self):
        """
        Get list of datasets
        Api reference: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list

        :return: list of dicts. Each dict contains two keys 'datasetId' and 'projectId'
        """
        request = '/datasets'
        response = self.connection.request(request, method='GET').object
        return [l['datasetReference'] for l in response['datasets']]

    def list_tables(self, dataset_id):
        """
        Get list of tables for dataset
        Api reference: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list

        :param dataset_id: str. Id of dataset.
        :return: list of dicts. Each dict contains next keys 'datasetId', 'projectId' and 'tableId'
        """
        request = '/datasets/%s/tables' % dataset_id
        response = self.connection.request(request, method='GET').object
        return [l['tableReference'] for l in response['tables']]

    def query(self, query, max_results=50000, timeout_ms=60000, use_legacy_sql=False):
        """
        Execute query and return result. Result will be chunked.

        Reference: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query

        :param query: str. BQ query. Example: SELECT * FROM {billing_table} LIMIT 1
        :param max_results: int. Page size
        :param timeout_ms: int. Max execution time. Default 1 min
        :param use_legacy_sql: bool. Specifies whether to use BigQuery's legacy SQL dialect for this query.

        :return: dict which represent row from result
        """
        request = '/queries'
        data = {'query': query,
                'useLegacySql': use_legacy_sql,
                'maxResults': max_results,
                'timeoutMs': timeout_ms}
        response = self.connection.request(request, method='POST', data=data).object
        query_job = QueryJob(response)
        return self._get_job_results(query_job, max_results, timeout_ms)

    def _get_job_results(self, query_job, max_results, timeout_ms):
        """
        Deal with paginated QueryJob results

        Reference: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults

        :param query_job: query job object

        :return: generator over rows
        """
        while True:
            for row in query_job.rows:
                yield row

            if not query_job.page_token:
                # last page
                break

            # next request
            data = {
                'maxResults': max_results,
                'pageToken': query_job.page_token,
                'timeoutMs': timeout_ms
            }
            request = '/queries/' + query_job.job_id

            response = self.connection.request(request, method='GET', params=data).object

            query_job = QueryJob(response)
