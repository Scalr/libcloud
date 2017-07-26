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

import base64
import hmac
import time

from hashlib import sha1

import libcloud.utils.py3
try:
    if libcloud.utils.py3.DEFAULT_LXML:
        from lxml.etree import Element, SubElement
    else:
        from xml.etree.ElementTree import Element, SubElement
except ImportError:
    from xml.etree.ElementTree import Element, SubElement

from libcloud.utils.py3 import httplib
from libcloud.utils.py3 import urlquote
from libcloud.utils.py3 import b
from libcloud.utils.py3 import tostring

from libcloud.utils.xml import fixxpath, findtext
from libcloud.utils.files import read_in_chunks
from libcloud.common.types import InvalidCredsError, LibcloudError
from libcloud.common.base import ConnectionUserAndKey, RawResponse
from libcloud.common.aws import AWSBaseResponse, AWSDriver, \
    AWSTokenConnection, SignedAWSConnection

from libcloud.storage.base import Object, Container, StorageDriver
from libcloud.storage.types import ContainerError
from libcloud.storage.types import ContainerIsNotEmptyError
from libcloud.storage.types import InvalidContainerNameError
from libcloud.storage.types import ContainerDoesNotExistError
from libcloud.storage.types import ObjectDoesNotExistError
from libcloud.storage.types import ObjectHashMismatchError


# How long before the token expires
EXPIRATION_SECONDS = 15 * 60

S3_US_STANDARD_HOST = 's3.amazonaws.com'
S3_US_EAST2_HOST = 's3-us-east-2.amazonaws.com'
S3_US_WEST_HOST = 's3-us-west-1.amazonaws.com'
S3_US_WEST_OREGON_HOST = 's3-us-west-2.amazonaws.com'
S3_US_GOV_WEST_HOST = 's3-us-gov-west-1.amazonaws.com'
S3_CN_NORTH_HOST = 's3.cn-north-1.amazonaws.com.cn'
S3_EU_WEST_HOST = 's3-eu-west-1.amazonaws.com'
S3_EU_WEST2_HOST = 's3-eu-west-2.amazonaws.com'
S3_EU_CENTRAL_HOST = 's3-eu-central-1.amazonaws.com'
S3_AP_SOUTH_HOST = 's3-ap-south-1.amazonaws.com'
S3_AP_SOUTHEAST_HOST = 's3-ap-southeast-1.amazonaws.com'
S3_AP_SOUTHEAST2_HOST = 's3-ap-southeast-2.amazonaws.com'
S3_AP_NORTHEAST1_HOST = 's3-ap-northeast-1.amazonaws.com'
S3_AP_NORTHEAST2_HOST = 's3-ap-northeast-2.amazonaws.com'
S3_AP_NORTHEAST_HOST = S3_AP_NORTHEAST1_HOST
S3_SA_EAST_HOST = 's3-sa-east-1.amazonaws.com'
S3_SA_SOUTHEAST2_HOST = 's3-sa-east-2.amazonaws.com'
S3_CA_CENTRAL_HOST = 's3-ca-central-1.amazonaws.com'

API_VERSION = '2006-03-01'
NAMESPACE = 'http://s3.amazonaws.com/doc/%s/' % (API_VERSION)

# AWS multi-part chunks must be minimum 5MB
CHUNK_SIZE = 5 * 1024 * 1024

# Desired number of items in each response inside a paginated request in
# ex_iterate_multipart_uploads.
RESPONSES_PER_REQUEST = 100


class S3Response(AWSBaseResponse):
    namespace = None
    valid_response_codes = [httplib.NOT_FOUND, httplib.CONFLICT,
                            httplib.BAD_REQUEST]

    def success(self):
        i = int(self.status)
        return i >= 200 and i <= 299 or i in self.valid_response_codes

    def parse_error(self):
        if self.status in [httplib.UNAUTHORIZED, httplib.FORBIDDEN]:
            raise InvalidCredsError(self.body)
        elif self.status == httplib.MOVED_PERMANENTLY:
            raise LibcloudError('This bucket is located in a different ' +
                                'region. Please use the correct driver.',
                                driver=S3StorageDriver)
        raise LibcloudError('Unknown error. Status code: %d' % (self.status),
                            driver=S3StorageDriver)


class S3RawResponse(S3Response, RawResponse):
    pass


class BaseS3Connection(ConnectionUserAndKey):
    """
    Represents a single connection to the S3 Endpoint
    """

    host = 's3.amazonaws.com'
    responseCls = S3Response
    rawResponseCls = S3RawResponse

    @staticmethod
    def get_auth_signature(method, headers, params, expires, secret_key, path,
                           vendor_prefix):
        """
        Signature = URL-Encode( Base64( HMAC-SHA1( YourSecretAccessKeyID,
                                    UTF-8-Encoding-Of( StringToSign ) ) ) );

        StringToSign = HTTP-VERB + "\n" +
            Content-MD5 + "\n" +
            Content-Type + "\n" +
            Expires + "\n" +
            CanonicalizedVendorHeaders +
            CanonicalizedResource;
        """
        special_headers = {'content-md5': '', 'content-type': '', 'date': ''}
        vendor_headers = {}

        for key, value in list(headers.items()):
            key_lower = key.lower()
            if key_lower in special_headers:
                special_headers[key_lower] = value.strip()
            elif key_lower.startswith(vendor_prefix):
                vendor_headers[key_lower] = value.strip()

        if expires:
            special_headers['date'] = str(expires)

        buf = [method]
        for _, value in sorted(special_headers.items()):
            buf.append(value)
        string_to_sign = '\n'.join(buf)

        buf = []
        for key, value in sorted(vendor_headers.items()):
            buf.append('%s:%s' % (key, value))
        header_string = '\n'.join(buf)

        values_to_sign = []
        for value in [string_to_sign, header_string, path]:
            if value:
                values_to_sign.append(value)

        string_to_sign = '\n'.join(values_to_sign)
        b64_hmac = base64.b64encode(
            hmac.new(b(secret_key), b(string_to_sign), digestmod=sha1).digest()
        )
        return b64_hmac.decode('utf-8')

    def add_default_params(self, params):
        expires = str(int(time.time()) + EXPIRATION_SECONDS)
        params['AWSAccessKeyId'] = self.user_id
        params['Expires'] = expires
        return params

    def pre_connect_hook(self, params, headers):
        params['Signature'] = self.get_auth_signature(
            method=self.method, headers=headers, params=params,
            expires=params['Expires'], secret_key=self.key, path=self.action,
            vendor_prefix=self.driver.http_vendor_prefix)
        return params, headers


class S3Connection(AWSTokenConnection, BaseS3Connection):
    """
    Represents a single connection to the S3 endpoint, with AWS-specific
    features.
    """
    pass


class S3SignatureV4Connection(SignedAWSConnection, BaseS3Connection):
    service_name = 's3'
    version = API_VERSION

    def __init__(self, user_id, key, secure=True, host=None, port=None,
                 url=None, timeout=None, proxy_url=None, token=None,
                 retry_delay=None, backoff=None):
        super(S3SignatureV4Connection, self).__init__(
            user_id, key, secure, host,
            port, url, timeout, proxy_url,
            token, retry_delay, backoff,
            4)  # force version 4


class S3MultipartUpload(object):
    """
    Class representing an amazon s3 multipart upload
    """

    def __init__(self, key, id, created_at, initiator, owner):
        """
        Class representing an amazon s3 multipart upload

        :param key: The object/key that was being uploaded
        :type key: ``str``

        :param id: The upload id assigned by amazon
        :type id: ``str``

        :param created_at: The date/time at which the upload was started
        :type created_at: ``str``

        :param initiator: The AWS owner/IAM user who initiated this
        :type initiator: ``str``

        :param owner: The AWS owner/IAM who will own this object
        :type owner: ``str``
        """
        self.key = key
        self.id = id
        self.created_at = created_at
        self.initiator = initiator
        self.owner = owner

    def __repr__(self):
        return ('<S3MultipartUpload: key=%s>' % (self.key))


class BaseS3StorageDriver(StorageDriver):
    name = 'Amazon S3 (standard)'
    website = 'http://aws.amazon.com/s3/'
    connectionCls = BaseS3Connection
    hash_type = 'md5'
    supports_chunked_encoding = False
    supports_s3_multipart_upload = True
    ex_location_name = ''
    namespace = NAMESPACE
    http_vendor_prefix = 'x-amz'

    def iterate_containers(self):
        response = self.connection.request('/')
        if response.status == httplib.OK:
            containers = self._to_containers(obj=response.object,
                                             xpath='Buckets/Bucket')
            return containers

        raise LibcloudError('Unexpected status code: %s' % (response.status),
                            driver=self)

    def list_container_objects(self, container, ex_prefix=None):
        """
        Return a list of objects for the given container.

        :param container: Container instance.
        :type container: :class:`Container`

        :param ex_prefix: Only return objects starting with ex_prefix
        :type ex_prefix: ``str``

        :return: A list of Object instances.
        :rtype: ``list`` of :class:`Object`
        """
        return list(self.iterate_container_objects(container,
                                                   ex_prefix=ex_prefix))

    def iterate_container_objects(self, container, ex_prefix=None):
        """
        Return a generator of objects for the given container.

        :param container: Container instance
        :type container: :class:`Container`

        :param ex_prefix: Only return objects starting with ex_prefix
        :type ex_prefix: ``str``

        :return: A generator of Object instances.
        :rtype: ``generator`` of :class:`Object`
        """
        params = {}
        if ex_prefix:
            params['prefix'] = ex_prefix

        last_key = None
        exhausted = False
        container_path = self._get_container_path(container)

        while not exhausted:
            if last_key:
                params['marker'] = last_key

            response = self.connection.request(container_path,
                                               params=params)

            if response.status != httplib.OK:
                raise LibcloudError('Unexpected status code: %s' %
                                    (response.status), driver=self)

            objects = self._to_objs(obj=response.object,
                                    xpath='Contents', container=container)
            is_truncated = response.object.findtext(fixxpath(
                xpath='IsTruncated', namespace=self.namespace)).lower()
            exhausted = (is_truncated == 'false')

            last_key = None
            for obj in objects:
                last_key = obj.name
                yield obj

    def get_container(self, container_name):
        try:
            response = self.connection.request('/%s' % container_name,
                                               method='HEAD')
            if response.status == httplib.NOT_FOUND:
                raise ContainerDoesNotExistError(value=None, driver=self,
                                                 container_name=container_name)
        except InvalidCredsError:
            # This just means the user doesn't have IAM permissions to do a
            # HEAD request but other requests might work.
            pass
        return Container(name=container_name, extra=None, driver=self)

    def get_object(self, container_name, object_name):
        container = self.get_container(container_name=container_name)
        object_path = self._get_object_path(container, object_name)
        response = self.connection.request(object_path, method='HEAD')

        if response.status == httplib.OK:
            obj = self._headers_to_object(object_name=object_name,
                                          container=container,
                                          headers=response.headers)
            return obj

        raise ObjectDoesNotExistError(value=None, driver=self,
                                      object_name=object_name)

    def _get_container_path(self, container):
        """
        Return a container path

        :param container: Container instance
        :type  container: :class:`Container`

        :return: A path for this container.
        :rtype: ``str``
        """
        return '/%s' % (container.name)

    def _get_object_path(self, container, object_name):
        """
        Return an object's CDN path.

        :param container: Container instance
        :type  container: :class:`Container`

        :param object_name: Object name
        :type  object_name: :class:`str`

        :return: A  path for this object.
        :rtype: ``str``
        """
        container_url = self._get_container_path(container)
        object_name_cleaned = self._clean_object_name(object_name)
        object_path = '%s/%s' % (container_url, object_name_cleaned)
        return object_path

    def create_container(self, container_name):
        if self.ex_location_name:
            root = Element('CreateBucketConfiguration')
            child = SubElement(root, 'LocationConstraint')
            child.text = self.ex_location_name

            data = tostring(root)
        else:
            data = ''

        response = self.connection.request('/%s' % (container_name),
                                           data=data,
                                           method='PUT')

        if response.status == httplib.OK:
            container = Container(name=container_name, extra=None, driver=self)
            return container
        elif response.status == httplib.CONFLICT:
            raise InvalidContainerNameError(
                value='Container with this name already exists. The name must '
                      'be unique among all the containers in the system',
                container_name=container_name, driver=self)
        elif response.status == httplib.BAD_REQUEST:
            raise ContainerError(
                value='Bad request when creating container: %s' %
                      response.body,
                container_name=container_name, driver=self)

        raise LibcloudError('Unexpected status code: %s' % (response.status),
                            driver=self)

    def delete_container(self, container):
        # Note: All the objects in the container must be deleted first
        response = self.connection.request('/%s' % (container.name),
                                           method='DELETE')
        if response.status == httplib.NO_CONTENT:
            return True
        elif response.status == httplib.CONFLICT:
            raise ContainerIsNotEmptyError(
                value='Container must be empty before it can be deleted.',
                container_name=container.name, driver=self)
        elif response.status == httplib.NOT_FOUND:
            raise ContainerDoesNotExistError(value=None,
                                             driver=self,
                                             container_name=container.name)

        return False

    def download_object(self, obj, destination_path, overwrite_existing=False,
                        delete_on_failure=True):
        obj_path = self._get_object_path(obj.container, obj.name)

        response = self.connection.request(obj_path, method='GET', raw=True)

        return self._get_object(obj=obj, callback=self._save_object,
                                response=response,
                                callback_kwargs={
                                    'obj': obj,
                                    'response': response.response,
                                    'destination_path': destination_path,
                                    'overwrite_existing': overwrite_existing,
                                    'delete_on_failure': delete_on_failure},
                                success_status_code=httplib.OK)

    def download_object_as_stream(self, obj, chunk_size=None):
        obj_path = self._get_object_path(obj.container, obj.name)
        response = self.connection.request(obj_path, method='GET',
                                           stream=True, raw=True)

        return self._get_object(
            obj=obj, callback=read_in_chunks,
            response=response,
            callback_kwargs={'iterator': response.iter_content(CHUNK_SIZE),
                             'chunk_size': chunk_size},
            success_status_code=httplib.OK)

    def upload_object(self, file_path, container, object_name, extra=None,
                      verify_hash=True, ex_storage_class=None):
        """
        @inherits: :class:`StorageDriver.upload_object`

        :param ex_storage_class: Storage class
        :type ex_storage_class: ``str``
        """
        return self._put_object(container=container, object_name=object_name,
                                extra=extra, file_path=file_path,
                                verify_hash=verify_hash,
                                storage_class=ex_storage_class)

    def _initiate_multipart(self, container, object_name, headers=None):
        """
        Initiates a multipart upload to S3

        :param container: The destination container
        :type container: :class:`Container`

        :param object_name: The name of the object which we are uploading
        :type object_name: ``str``

        :keyword headers: Additional headers to send with the request
        :type headers: ``dict``

        :return: The id of the newly created multipart upload
        :rtype: ``str``
        """
        headers = headers or {}

        request_path = self._get_object_path(container, object_name)
        params = {'uploads': ''}

        response = self.connection.request(request_path, method='POST',
                                           headers=headers, params=params)

        if response.status != httplib.OK:
            raise LibcloudError('Error initiating multipart upload',
                                driver=self)

        return findtext(element=response.object, xpath='UploadId',
                        namespace=self.namespace)

    def _upload_multipart_chunks(self, container, object_name, upload_id,
                                 stream, calculate_hash=True):
        """
        Uploads data from an iterator in fixed sized chunks to S3

        :param container: The destination container
        :type container: :class:`Container`

        :param object_name: The name of the object which we are uploading
        :type object_name: ``str``

        :param upload_id: The upload id allocated for this multipart upload
        :type upload_id: ``str``

        :param stream: The generator for fetching the upload data
        :type stream: ``generator``

        :keyword calculate_hash: Indicates if we must calculate the data hash
        :type calculate_hash: ``bool``

        :return: A tuple of (chunk info, checksum, bytes transferred)
        :rtype: ``tuple``
        """
        data_hash = None
        if calculate_hash:
            data_hash = self._get_hash_function()

        bytes_transferred = 0
        count = 1
        chunks = []
        params = {'uploadId': upload_id}

        request_path = self._get_object_path(container, object_name)

        # Read the input data in chunk sizes suitable for AWS
        for data in read_in_chunks(stream, chunk_size=CHUNK_SIZE,
                                   fill_size=True, yield_empty=True):
            bytes_transferred += len(data)

            if calculate_hash:
                data_hash.update(data)

            chunk_hash = self._get_hash_function()
            chunk_hash.update(data)
            chunk_hash = base64.b64encode(chunk_hash.digest()).decode('utf-8')

            # The Content-MD5 header provides an extra level of data check and
            # is recommended by amazon
            headers = {
                'Content-Length': len(data),
                'Content-MD5': chunk_hash,
            }

            params['partNumber'] = count

            resp = self.connection.request(request_path, method='PUT',
                                           data=data, headers=headers,
                                           params=params)

            if resp.status != httplib.OK:
                raise LibcloudError('Error uploading chunk', driver=self)

            server_hash = resp.headers['etag'].replace('"', '')

            # Keep this data for a later commit
            chunks.append((count, server_hash))
            count += 1

        if calculate_hash:
            data_hash = data_hash.hexdigest()

        return (chunks, data_hash, bytes_transferred)

    def _commit_multipart(self, container, object_name, upload_id, chunks):
        """
        Makes a final commit of the data.

        :param container: The destination container
        :type container: :class:`Container`

        :param object_name: The name of the object which we are uploading
        :type object_name: ``str``

        :param upload_id: The upload id allocated for this multipart upload
        :type upload_id: ``str``

        :param chunks: A list of (chunk_number, chunk_hash) tuples.
        :type chunks: ``list``

        :return: The server side hash of the uploaded data
        :rtype: ``str``
        """
        root = Element('CompleteMultipartUpload')

        for (count, etag) in chunks:
            part = SubElement(root, 'Part')
            part_no = SubElement(part, 'PartNumber')
            part_no.text = str(count)

            etag_id = SubElement(part, 'ETag')
            etag_id.text = str(etag)

        data = tostring(root)

        headers = {'Content-Length': len(data)}
        params = {'uploadId': upload_id}
        request_path = self._get_object_path(container, object_name)
        response = self.connection.request(request_path, headers=headers,
                                           params=params, data=data,
                                           method='POST')

        if response.status != httplib.OK:
            element = response.object
            # pylint: disable=maybe-no-member
            code, message = response._parse_error_details(element=element)
            msg = 'Error in multipart commit: %s (%s)' % (message, code)
            raise LibcloudError(msg, driver=self)

        # Get the server's etag to be passed back to the caller
        body = response.parse_body()
        server_hash = body.find(fixxpath(xpath='ETag',
                                         namespace=self.namespace)).text
        return server_hash

    def _abort_multipart(self, container, object_name, upload_id):
        """
        Aborts an already initiated multipart upload

        :param container: The destination container
        :type container: :class:`Container`

        :param object_name: The name of the object which we are uploading
        :type object_name: ``str``

        :param upload_id: The upload id allocated for this multipart upload
        :type upload_id: ``str``
        """

        params = {'uploadId': upload_id}
        request_path = self._get_object_path(container, object_name)

        resp = self.connection.request(request_path, method='DELETE',
                                       params=params)

        if resp.status != httplib.NO_CONTENT:
            raise LibcloudError('Error in multipart abort. status_code=%d' %
                                (resp.status), driver=self)

    def upload_object_via_stream(self, iterator, container, object_name,
                                 extra=None, ex_storage_class=None):
        """
        @inherits: :class:`StorageDriver.upload_object_via_stream`

        :param ex_storage_class: Storage class
        :type ex_storage_class: ``str``
        """

        method = 'PUT'
        params = None

        # This driver is used by other S3 API compatible drivers also.
        # Amazon provides a different (complex?) mechanism to do multipart
        # uploads
        if self.supports_s3_multipart_upload:
            return self._put_object_multipart(container=container,
                                              object_name=object_name,
                                              extra=extra,
                                              stream=iterator,
                                              verify_hash=False,
                                              storage_class=ex_storage_class)
        return self._put_object(container=container, object_name=object_name,
                                extra=extra, method=method, query_args=params,
                                stream=iterator, verify_hash=False,
                                storage_class=ex_storage_class)

    def delete_object(self, obj):
        object_path = self._get_object_path(obj.container, obj.name)
        response = self.connection.request(object_path, method='DELETE')
        if response.status == httplib.NO_CONTENT:
            return True
        elif response.status == httplib.NOT_FOUND:
            raise ObjectDoesNotExistError(value=None, driver=self,
                                          object_name=obj.name)

        return False

    def ex_iterate_multipart_uploads(self, container, prefix=None,
                                     delimiter=None):
        """
        Extension method for listing all in-progress S3 multipart uploads.

        Each multipart upload which has not been committed or aborted is
        considered in-progress.

        :param container: The container holding the uploads
        :type container: :class:`Container`

        :keyword prefix: Print only uploads of objects with this prefix
        :type prefix: ``str``

        :keyword delimiter: The object/key names are grouped based on
            being split by this delimiter
        :type delimiter: ``str``

        :return: A generator of S3MultipartUpload instances.
        :rtype: ``generator`` of :class:`S3MultipartUpload`
        """

        if not self.supports_s3_multipart_upload:
            raise LibcloudError('Feature not supported', driver=self)

        # Get the data for a specific container
        request_path = self._get_container_path(container)
        params = {'max-uploads': RESPONSES_PER_REQUEST, 'uploads': ''}

        if prefix:
            params['prefix'] = prefix

        if delimiter:
            params['delimiter'] = delimiter

        def finder(node, text):
            return node.findtext(fixxpath(xpath=text,
                                          namespace=self.namespace))

        while True:
            response = self.connection.request(request_path, params=params)

            if response.status != httplib.OK:
                raise LibcloudError('Error fetching multipart uploads. '
                                    'Got code: %s' % response.status,
                                    driver=self)

            body = response.parse_body()
            # pylint: disable=maybe-no-member
            for node in body.findall(fixxpath(xpath='Upload',
                                              namespace=self.namespace)):

                initiator = node.find(fixxpath(xpath='Initiator',
                                               namespace=self.namespace))
                owner = node.find(fixxpath(xpath='Owner',
                                           namespace=self.namespace))

                key = finder(node, 'Key')
                upload_id = finder(node, 'UploadId')
                created_at = finder(node, 'Initiated')
                initiator = finder(initiator, 'DisplayName')
                owner = finder(owner, 'DisplayName')

                yield S3MultipartUpload(key, upload_id, created_at,
                                        initiator, owner)

            # Check if this is the last entry in the listing
            # pylint: disable=maybe-no-member
            is_truncated = body.findtext(fixxpath(xpath='IsTruncated',
                                                  namespace=self.namespace))

            if is_truncated.lower() == 'false':
                break

            # Provide params for the next request
            upload_marker = body.findtext(fixxpath(xpath='NextUploadIdMarker',
                                                   namespace=self.namespace))
            key_marker = body.findtext(fixxpath(xpath='NextKeyMarker',
                                                namespace=self.namespace))

            params['key-marker'] = key_marker
            params['upload-id-marker'] = upload_marker

    def ex_cleanup_all_multipart_uploads(self, container, prefix=None):
        """
        Extension method for removing all partially completed S3 multipart
        uploads.

        :param container: The container holding the uploads
        :type container: :class:`Container`

        :keyword prefix: Delete only uploads of objects with this prefix
        :type prefix: ``str``
        """

        # Iterate through the container and delete the upload ids
        for upload in self.ex_iterate_multipart_uploads(container, prefix,
                                                        delimiter=None):
            self._abort_multipart(container, upload.key, upload.id)

    def _clean_object_name(self, name):
        name = urlquote(name)
        return name

    def _put_object(self, container, object_name, method='PUT',
                    query_args=None, extra=None, file_path=None,
                    stream=None, verify_hash=True, storage_class=None):
        headers = {}
        extra = extra or {}

        headers.update(self._to_storage_class_headers(storage_class))

        content_type = extra.get('content_type', None)
        meta_data = extra.get('meta_data', None)
        acl = extra.get('acl', None)

        if meta_data:
            for key, value in list(meta_data.items()):
                key = self.http_vendor_prefix + '-meta-%s' % (key)
                headers[key] = value

        if acl:
            headers[self.http_vendor_prefix + '-acl'] = acl

        request_path = self._get_object_path(container, object_name)

        if query_args:
            request_path = '?'.join((request_path, query_args))

        result_dict = self._upload_object(
            object_name=object_name, content_type=content_type,
            request_path=request_path, request_method=method,
            headers=headers, file_path=file_path, stream=stream)

        response = result_dict['response']
        bytes_transferred = result_dict['bytes_transferred']
        headers = response.headers
        response = response
        server_hash = headers.get('etag', '').replace('"', '')

        if (verify_hash and result_dict['data_hash'] != server_hash):
            raise ObjectHashMismatchError(
                value='MD5 hash {0} checksum does not match {1}'.format(
                    server_hash, result_dict['data_hash']),
                object_name=object_name, driver=self)
        elif response.status == httplib.OK:
            obj = Object(
                name=object_name, size=bytes_transferred, hash=server_hash,
                extra={'acl': acl}, meta_data=meta_data, container=container,
                driver=self)

            return obj
        else:
            raise LibcloudError(
                'Unexpected status code, status_code=%s' % (response.status),
                driver=self)

    def _put_object_multipart(self, container, object_name, stream,
                              extra=None, verify_hash=False,
                              storage_class=None):
        """
        Uploads an object using the S3 multipart algorithm.

        :param container: The destination container
        :type container: :class:`Container`

        :param object_name: The name of the object which we are uploading
        :type object_name: ``str``

        :param stream: The generator for fetching the upload data
        :type stream: ``generator``

        :keyword verify_hash: Indicates if we must calculate the data hash
        :type verify_hash: ``bool``

        :keyword extra: Additional options
        :type extra: ``dict``

        :keyword storage_class: The name of the S3 object's storage class
        :type extra: ``str``

        :return: The uploaded object
        :rtype: :class:`Object`
        """
        headers = {}
        extra = extra or {}

        headers.update(self._to_storage_class_headers(storage_class))

        content_type = extra.get('content_type', None)
        meta_data = extra.get('meta_data', None)
        acl = extra.get('acl', None)

        if content_type:
            headers['Content-Type'] = content_type

        if meta_data:
            for key, value in list(meta_data.items()):
                key = self.http_vendor_prefix + '-meta-%s' % (key)
                headers[key] = value

        if acl:
            headers[self.http_vendor_prefix + '-acl'] = acl

        upload_id = self._initiate_multipart(container, object_name,
                                             headers=headers)

        try:
            result = self._upload_multipart_chunks(container, object_name,
                                                   upload_id, stream,
                                                   calculate_hash=verify_hash)
            chunks, data_hash, bytes_transferred = result

            # Commit the chunk info and complete the upload
            etag = self._commit_multipart(container, object_name, upload_id,
                                          chunks)
        except Exception:
            # Amazon provides a mechanism for aborting an upload.
            self._abort_multipart(container, object_name, upload_id)
            raise

        return Object(
            name=object_name, size=bytes_transferred, hash=etag,
            extra={'acl': acl}, meta_data=meta_data, container=container,
            driver=self)

    def _to_storage_class_headers(self, storage_class):
        """
        Generates request headers given a storage class name.

        :keyword storage_class: The name of the S3 object's storage class
        :type extra: ``str``

        :return: Headers to include in a request
        :rtype: :dict:
        """
        headers = {}
        storage_class = storage_class or 'standard'
        if storage_class not in ['standard', 'reduced_redundancy']:
            raise ValueError(
                'Invalid storage class value: %s' % (storage_class))

        key = self.http_vendor_prefix + '-storage-class'
        headers[key] = storage_class.upper()
        return headers

    def _to_containers(self, obj, xpath):
        for element in obj.findall(fixxpath(xpath=xpath,
                                            namespace=self.namespace)):
            yield self._to_container(element)

    def _to_objs(self, obj, xpath, container):
        return [self._to_obj(element, container) for element in
                obj.findall(fixxpath(xpath=xpath, namespace=self.namespace))]

    def _to_container(self, element):
        extra = {
            'creation_date': findtext(element=element, xpath='CreationDate',
                                      namespace=self.namespace)
        }

        container = Container(name=findtext(element=element, xpath='Name',
                                            namespace=self.namespace),
                              extra=extra,
                              driver=self
                              )

        return container

    def _headers_to_object(self, object_name, container, headers):
        hash = headers['etag'].replace('"', '')
        extra = {'content_type': headers['content-type'],
                 'etag': headers['etag']}
        meta_data = {}

        if 'last-modified' in headers:
            extra['last_modified'] = headers['last-modified']

        for key, value in headers.items():
            if not key.lower().startswith(self.http_vendor_prefix + '-meta-'):
                continue

            key = key.replace(self.http_vendor_prefix + '-meta-', '')
            meta_data[key] = value

        obj = Object(name=object_name, size=headers['content-length'],
                     hash=hash, extra=extra,
                     meta_data=meta_data,
                     container=container,
                     driver=self)
        return obj

    def _to_obj(self, element, container):
        owner_id = findtext(element=element, xpath='Owner/ID',
                            namespace=self.namespace)
        owner_display_name = findtext(element=element,
                                      xpath='Owner/DisplayName',
                                      namespace=self.namespace)
        meta_data = {'owner': {'id': owner_id,
                               'display_name': owner_display_name}}
        last_modified = findtext(element=element,
                                 xpath='LastModified',
                                 namespace=self.namespace)
        extra = {'last_modified': last_modified}

        obj = Object(name=findtext(element=element, xpath='Key',
                                   namespace=self.namespace),
                     size=int(findtext(element=element, xpath='Size',
                                       namespace=self.namespace)),
                     hash=findtext(element=element, xpath='ETag',
                                   namespace=self.namespace).replace('"', ''),
                     extra=extra,
                     meta_data=meta_data,
                     container=container,
                     driver=self
                     )

        return obj


class S3StorageDriver(AWSDriver, BaseS3StorageDriver):
    name = 'Amazon S3 (us-east-1)'
    connectionCls = S3SignatureV4Connection
    region_name = 'us-east-1'


class S3USEast2Connection(S3SignatureV4Connection):
    host = S3_US_EAST2_HOST


class S3USEast2StorageDriver(S3StorageDriver):
    name = 'Amazon S3 (us-east-2)'
    connectionCls = S3USEast2Connection
    ex_location_name = 'us-east-2'
    region_name = 'us-east-2'


class S3USWestConnection(S3SignatureV4Connection):
    host = S3_US_WEST_HOST


class S3USWestStorageDriver(S3StorageDriver):
    name = 'Amazon S3 (us-west-1)'
    connectionCls = S3USWestConnection
    ex_location_name = 'us-west-1'
    region_name = 'us-west-1'


class S3USWestOregonConnection(S3SignatureV4Connection):
    host = S3_US_WEST_OREGON_HOST


class S3USWestOregonStorageDriver(S3StorageDriver):
    name = 'Amazon S3 (us-west-2)'
    connectionCls = S3USWestOregonConnection
    ex_location_name = 'us-west-2'
    region_name = 'us-west-2'


class S3USGovWestConnection(S3SignatureV4Connection):
    host = S3_US_GOV_WEST_HOST


class S3USGovWestStorageDriver(S3StorageDriver):
    name = 'Amazon S3 (us-gov-west-1)'
    connectionCls = S3USGovWestConnection
    ex_location_name = 'us-gov-west-1'
    region_name = 'us-gov-west-1'


class S3CNNorthConnection(S3SignatureV4Connection):
    host = S3_CN_NORTH_HOST


class S3CNNorthStorageDriver(S3StorageDriver):
    name = 'Amazon S3 (cn-north-1)'
    connectionCls = S3CNNorthConnection
    ex_location_name = 'cn-north-1'
    region_name = 'cn-north-1'


class S3EUWestConnection(S3SignatureV4Connection):
    host = S3_EU_WEST_HOST


class S3EUWestStorageDriver(S3StorageDriver):
    name = 'Amazon S3 (eu-west-1)'
    connectionCls = S3EUWestConnection
    ex_location_name = 'EU'
    region_name = 'eu-west-1'


class S3EUWest2Connection(S3SignatureV4Connection):
    host = S3_EU_WEST2_HOST


class S3EUWest2StorageDriver(S3StorageDriver):
    name = 'Amazon S3 (eu-west-2)'
    connectionCls = S3EUWest2Connection
    ex_location_name = 'eu-west-2'
    region_name = 'eu-west-2'


class S3EUCentralConnection(S3SignatureV4Connection):
    host = S3_EU_CENTRAL_HOST


class S3EUCentralStorageDriver(S3StorageDriver):
    name = 'Amazon S3 (eu-central-1)'
    connectionCls = S3EUCentralConnection
    ex_location_name = 'eu-central-1'
    region_name = 'eu-central-1'


class S3APSEConnection(S3SignatureV4Connection):
    host = S3_AP_SOUTHEAST_HOST


class S3APSEStorageDriver(S3StorageDriver):
    name = 'Amazon S3 (ap-southeast-1)'
    connectionCls = S3APSEConnection
    ex_location_name = 'ap-southeast-1'
    region_name = 'ap-southeast-1'


class S3APSE2Connection(S3SignatureV4Connection):
    host = S3_AP_SOUTHEAST2_HOST


class S3APSE2StorageDriver(S3StorageDriver):
    name = 'Amazon S3 (ap-southeast-2)'
    connectionCls = S3APSE2Connection
    ex_location_name = 'ap-southeast-2'
    region_name = 'ap-southeast-2'


class S3APNE1Connection(S3SignatureV4Connection):
    host = S3_AP_NORTHEAST1_HOST

S3APNEConnection = S3APNE1Connection


class S3APNE1StorageDriver(S3StorageDriver):
    name = 'Amazon S3 (ap-northeast-1)'
    connectionCls = S3APNEConnection
    ex_location_name = 'ap-northeast-1'
    region_name = 'ap-northeast-1'

S3APNEStorageDriver = S3APNE1StorageDriver


class S3APNE2Connection(S3SignatureV4Connection):
    host = S3_AP_NORTHEAST2_HOST


class S3APNE2StorageDriver(S3StorageDriver):
    name = 'Amazon S3 (ap-northeast-2)'
    connectionCls = S3APNE2Connection
    ex_location_name = 'ap-northeast-2'
    region_name = 'ap-northeast-2'


class S3APSouthConnection(S3SignatureV4Connection):
    host = S3_AP_SOUTH_HOST


class S3APSouthStorageDriver(S3StorageDriver):
    name = 'Amazon S3 (ap-south-1)'
    connectionCls = S3APSouthConnection
    ex_location_name = 'ap-south-1'
    region_name = 'ap-south-1'


class S3SAEastConnection(S3SignatureV4Connection):
    host = S3_SA_EAST_HOST


class S3SAEastStorageDriver(S3StorageDriver):
    name = 'Amazon S3 (sa-east-1)'
    connectionCls = S3SAEastConnection
    ex_location_name = 'sa-east-1'
    region_name = 'sa-east-1'


class S3CACentralConnection(S3SignatureV4Connection):
    host = S3_CA_CENTRAL_HOST


class S3CACentralStorageDriver(S3StorageDriver):
    name = 'Amazon S3 (ca-central-1)'
    connectionCls = S3CACentralConnection
    ex_location_name = 'ca-central-1'
    region_name = 'ca-central-1'
