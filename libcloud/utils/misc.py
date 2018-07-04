# -*- coding: utf-8 -*-
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
from __future__ import absolute_import, division

import binascii
import collections
import itertools
import logging
import os
import socket
import ssl
import sys
import time
from datetime import datetime
from datetime import timedelta
from functools import wraps

from libcloud.common.exceptions import RateLimitReachedError
from libcloud.common.providers import get_driver as _get_driver
from libcloud.common.providers import set_driver as _set_driver
from libcloud.utils.py3 import httplib

__all__ = [
    'find',
    'get_driver',
    'set_driver',
    'merge_valid_keys',
    'get_new_obj',
    'str2dicts',
    'dict2str',
    'reverse_dict',
    'lowercase_keys',
    'get_secure_random_string',
    'retry',

    'ReprMixin',
    'PageList'
]

# Error message which indicates a transient SSL error upon which request
# can be retried
TRANSIENT_SSL_ERROR = 'The read operation timed out'

LOG = logging.getLogger(__name__)


class TransientSSLError(ssl.SSLError):
    """Represent transient SSL errors, e.g. timeouts"""
    pass


# Constants used by the ``retry`` decorator
DEFAULT_TIMEOUT = 30  # default retry timeout
DEFAULT_DELAY = 1  # default sleep delay used in each iterator
DEFAULT_BACKOFF = 1  # retry backup multiplier
RETRY_EXCEPTIONS = (RateLimitReachedError, socket.error, socket.gaierror,
                    httplib.NotConnected, httplib.ImproperConnectionState,
                    TransientSSLError)


def find(l, predicate):
    results = [x for x in l if predicate(x)]
    return results[0] if len(results) > 0 else None


# Note: Those are aliases for backward-compatibility for functions which have
# been moved to "libcloud.common.providers" module
get_driver = _get_driver
set_driver = _set_driver


def merge_valid_keys(params, valid_keys, extra):
    """
    Merge valid keys from extra into params dictionary and return
    dictionary with keys which have been merged.

    Note: params is modified in place.
    """
    merged = {}
    if not extra:
        return merged

    for key in valid_keys:
        if key in extra:
            params[key] = extra[key]
            merged[key] = extra[key]

    return merged


def get_new_obj(obj, klass, attributes):
    """
    Pass attributes from the existing object 'obj' and attributes
    dictionary to a 'klass' constructor.
    Attributes from 'attributes' dictionary are only passed to the
    constructor if they are not None.
    """
    kwargs = {}
    for key, value in list(obj.__dict__.items()):
        if isinstance(value, dict):
            kwargs[key] = value.copy()
        elif isinstance(value, (tuple, list)):
            kwargs[key] = value[:]
        else:
            kwargs[key] = value

    for key, value in list(attributes.items()):
        if value is None:
            continue

        if isinstance(value, dict):
            kwargs_value = kwargs.get(key, {})
            for key1, value2 in list(value.items()):
                if value2 is None:
                    continue

                kwargs_value[key1] = value2
            kwargs[key] = kwargs_value
        else:
            kwargs[key] = value

    return klass(**kwargs)


def str2dicts(data):
    """
    Create a list of dictionaries from a whitespace and newline delimited text.

    For example, this:
    cpu 1100
    ram 640

    cpu 2200
    ram 1024

    becomes:
    [{'cpu': '1100', 'ram': '640'}, {'cpu': '2200', 'ram': '1024'}]
    """
    list_data = []
    list_data.append({})
    d = list_data[-1]

    lines = data.split('\n')
    for line in lines:
        line = line.strip()

        if not line:
            d = {}
            list_data.append(d)
            d = list_data[-1]
            continue

        whitespace = line.find(' ')

        if not whitespace:
            continue

        key = line[0:whitespace]
        value = line[whitespace + 1:]
        d.update({key: value})

    list_data = [val for val in list_data if val != {}]
    return list_data


def str2list(data):
    """
    Create a list of values from a whitespace and newline delimited text
    (keys are ignored).

    For example, this:
    ip 1.2.3.4
    ip 1.2.3.5
    ip 1.2.3.6

    becomes:
    ['1.2.3.4', '1.2.3.5', '1.2.3.6']
    """
    list_data = []

    for line in data.split('\n'):
        line = line.strip()

        if not line:
            continue

        try:
            splitted = line.split(' ')
            # key = splitted[0]
            value = splitted[1]
        except Exception:
            continue

        list_data.append(value)

    return list_data


def dict2str(data):
    """
    Create a string with a whitespace and newline delimited text from a
    dictionary.

    For example, this:
    {'cpu': '1100', 'ram': '640', 'smp': 'auto'}

    becomes:
    cpu 1100
    ram 640
    smp auto

    cpu 2200
    ram 1024
    """
    result = ''
    for k in data:
        if data[k] is not None:
            result += '%s %s\n' % (str(k), str(data[k]))
        else:
            result += '%s\n' % str(k)

    return result


def reverse_dict(dictionary):
    return dict([(value, key) for key, value in list(dictionary.items())])


def lowercase_keys(dictionary):
    return dict(((k.lower(), v) for k, v in dictionary.items()))


def repeat_last(iterable):
    """
    Iterates over the sequence and repeats the last element in forever loop.

    :param iterable: The sequence to iterate on.
    :type iterable: :class:`collections.Iterable`

    :rtype: :class:`types.GeneratorType`
    """
    iterable = iter(iterable)
    # Will throw StopItreation in case iterable is
    # empty (similar to itertools.cycle)
    item = next(iterable)
    yield item
    for item in iterable:
        yield item
    while True:
        yield item


def total_seconds(td):
    """
    Total seconds in the duration.

    :type td: :class:`timedelta`
    """
    # Keep backward compatibility with Python 2.6 which
    # doesn't have this method
    if hasattr(td, 'total_seconds'):
        return td.total_seconds()
    else:
        return ((td.days * 86400 + td.seconds) * 10**6 +
                td.microseconds) / 10**6


def get_secure_random_string(size):
    """
    Return a string of ``size`` random bytes. Returned string is suitable for
    cryptographic use.

    :param size: Size of the generated string.
    :type size: ``int``

    :return: Random string.
    :rtype: ``str``
    """
    value = os.urandom(size)
    value = binascii.hexlify(value)
    value = value.decode('utf-8')[:size]
    return value


class ReprMixin(object):
    """
    Mixin class which adds __repr__ and __str__ methods for the attributes
    specified on the class.
    """

    _repr_attributes = []

    def __repr__(self):
        attributes = []
        for attribute in self._repr_attributes:
            value = getattr(self, attribute, None)
            attributes.append('%s=%s' % (attribute, value))

        values = (self.__class__.__name__, ', '.join(attributes))
        result = '<%s %s>' % values
        return result

    def __str__(self):
        return str(self.__repr__())


def retry(retry_exceptions=None, retry_delay=None, timeout=None,
          backoff=None):
    """
    Retry decorator that helps to handle common transient exceptions.

    :param retry_delay: retry delay between the attempts.
    :type retry_delay: int or :class:`collections.Sequence[int]`

    :param backoff: the denominator of a geometric progression
        (:math:`retry\_delay_n = retry\_delay × backoff^{n-1}`).
    :type backoff: int

    :param timeout: maximum time to wait.
    :type timeout: int

    :param retry_exceptions: types of exceptions to retry on.
    :type retry_exceptions: tuple of :class:`Exception`

    :Example:

    >>> retry_request = retry(
    >>>     timeout=1,
    >>>     retry_delay=1,
    >>>     backoff=1)
    >>> retry_request(connection.request)()
    """
    if retry_exceptions is None:
        retry_exceptions = RETRY_EXCEPTIONS
    if retry_delay is None or (
            isinstance(retry_delay, collections.Sequence) and
            len(retry_delay) == 0):
        retry_delay = DEFAULT_DELAY
    if timeout is None:
        timeout = DEFAULT_TIMEOUT
    if backoff is None:
        backoff = DEFAULT_BACKOFF

    timeout = max(timeout, 0)

    def transform_ssl_error(func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ssl.SSLError:
            exc = sys.exc_info()[1]

            if TRANSIENT_SSL_ERROR in str(exc):
                raise TransientSSLError(*exc.args)

            raise exc

    def decorator(func):
        @wraps(func)
        def retry_loop(*args, **kwargs):
            retry_msg = "Server returned %r, retrying request " \
                        "in %s seconds ..."
            end_time = datetime.now() + timedelta(seconds=timeout)

            if isinstance(retry_delay, collections.Sequence):
                retry_time_progression = repeat_last(retry_delay)
            else:
                retry_time_progression = (
                    retry_delay * (backoff ** i) for i in itertools.count()
                )

            for delay in retry_time_progression:
                try:
                    return transform_ssl_error(func, *args, **kwargs)
                except retry_exceptions as exc:
                    to_timeout = total_seconds(end_time - datetime.now())
                    if to_timeout <= 0:
                        raise
                    if isinstance(exc, RateLimitReachedError) \
                            and exc.retry_after:
                        LOG.debug(retry_msg, exc, exc.retry_after)
                        time.sleep(exc.retry_after)
                        return retry_loop(*args, **kwargs)
                    delay = min(delay, to_timeout)
                    LOG.debug(retry_msg, exc, delay)
                    time.sleep(delay)
        return retry_loop
    return decorator


class UnknownScope(Exception):
    """ Error to signalize unknown scope in PageList.apply"""


class PageList(object):
    """
    An Iterator that wraps list request functions to encapsulate pagination.

    Usage example (this class needs to be inherited to implement
        page_token_name, page_size_name, and next_page_token()
        for the pagination to be possible):
    1. Treat pager as unbroken list of elements
    >>> node_pager = SomeCloudPageList(
            connection.request,
            (arg1,),
            {'kwarg1': val1,
             'kwarg2': val2},
            page_size=20,
            process_fn=lambda x: x.split(';'))
    >>> for node in node_pager:  # node_pager will automatically "switch" pages
            save_node_info(node)

    2. Process elements page by page. (Note: the request may return next_token,
        but the next page may be empty. In that case page() will return []
        and only on the succeeding call it will return None)
    >>> node_pager = SomeCloudPageList(
            connection.request,
            (arg1,),
            {'kwarg1': val1,
             'kwarg2': val2},
            page_size=20,
            process_fn=lambda x: x.split(';'))
    >>> current_page = node_pager.page()
    >>> while current_page:
            for node in current_page:
                do_work(node)
            signal_batch_finished()
            current_page = node_pager.page()
    """
    page_token_name = None
    page_size_name = None

    def __init__(self, request_fn, request_args, request_kwargs, page_size=0,
            process_fn=None):
        """
        :param function request_fn: Function that accepts pagination parameter.
        :param list request_args: List of function arguments.
        :param dict request_kwargs: Dict of function keyword arguments
        :param function process_fn: Function to be applied to return value of
            `request_fn`. It must produce list of elements.
        """
        self.request_fn = request_fn
        self.request_args = request_args
        self.request_kwargs = request_kwargs
        self.page_size = page_size
        self.process_fn = process_fn
        self.next_page_token = None
        self.current_page = None
        self.applied_functions = []

    def __add__(self, other):
        """
        Will fetch elements from self, then from other
        """
        return PageListSum([self, other])

    def apply(self, func, scope='element'):
        """
        Applies function to the page.

        :param function func: function to be applied. Must accept 1 argument.
        :param str scope: If scope is 'element' func will be applied to each
            element. func must return an updated or new element.
            If scope is 'page', then func will receive the whole
            page as a list and the func must return a list.
        """
        if scope not in ['element', 'page']:
            raise UnknownScope(
                'The scope should be "element" or "page", got {}'
                .format(scope))
        self.applied_functions.append((func, scope))

    def extract_next_page_token(self, response):
        """
        Returns value of the next page token/marker from the response.
        Must return None to signalize that there are no more pages to request.

        Subclasses need specify page_token_name and to implement this method
        in order for page() method to know how to select pages.

        :param object response: Response returned by `request_function()`.
        """
        raise NotImplementedError()

    def update_request_kwds(self):
        """
        Updates token/identifier parameter of the next page and page size
        for `request_function()`.
        """
        if self.page_token_name is not None:
            self.request_kwargs[self.page_token_name] = self.next_page_token
        if self.page_size_name is not None:
            self.request_kwargs[self.page_size_name] = self.page_size

    def has_more(self):
        """
        Returns True if there is still data to request.
        """
        return self.current_page is None \
            or self.next_page_token is not None

    def page(self):
        """
        Requests/lists next page of data. Applies next page token automatically
        and transform function to the result of the request.
        """
        if not self.has_more():
            return None

        self.update_request_kwds()
        response = self.request_fn(*self.request_args, **self.request_kwargs)

        # If response is not a list, there should be process_fn to break response
        # into elements and return a list of them.
        if self.process_fn:
            self.current_page = self.process_fn(response)
        else:
            self.current_page = response

        # Applying delayed functions to the page.
        for f, scope in self.applied_functions:
            if scope == 'element':
                self.current_page = [f(el) for el in self.current_page]
            elif scope == 'page':
                self.current_page = f(self.current_page)
            else:
                raise UnknownScope(
                    'The scope should be "element" or "page", got {}'
                    .format(scope))

        # Saving page token for later use
        self.next_page_token = self.extract_next_page_token(response)

        return self.current_page

    def __iter__(self):
        """
        Iterates over elements of all pages.
        """
        while self.has_more():
            for item in self.page():
                yield item


class EmptyPageList(PageList):
    """
    Empty PageList iterator useful for when adding PageList together.
    """

    def __init__(self):
        super(EmptyPageList, self).__init__(lambda: [], (), {})

    def extract_next_page_token(self):
        return None


class PageListSum(PageList):
    """
    Iterator over PageLists
    """

    def __init__(self, page_lists=None):
        self.page_lists = page_lists or []

    def __add__(self, other):
        """
        Will fetch elements from self, then from other
        """
        self.page_lists.append(other)
        return self

    def has_more(self):
        return any(pl.has_more() for pl in self.page_lists)

    def page(self):
        result = None
        for pl in self.page_lists:
            if pl.has_more():
                result = pl.page()
                if result:
                    break
        return result

    @classmethod
    def multiple_from_fn_iterator(cls, fn_iterator, *args, **kwargs):
        """
        Produces multiple PageLists for each function in fn_iterator.
        """
        return [cls(request_fn, *args, **kwargs) for request_fn in fn_iterator]
