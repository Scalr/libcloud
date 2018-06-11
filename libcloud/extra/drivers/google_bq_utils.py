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
Module with support utils for Google Big Data Driver.
"""
import datetime
import base64
import decimal

from pytz import UTC


_RFC3339_MICROS_NO_ZULU = '%Y-%m-%dT%H:%M:%S.%f'
_RFC3339_NO_FRACTION = '%Y-%m-%dT%H:%M:%S'
_YEAR_MONTH_DAY = '%Y-%m-%d'
_EPOCH = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=UTC)


def _date_from_iso8601_date(value):
    """Convert a ISO8601 date string to native datetime date

    :type value: str
    :param value: The date string to convert

    :rtype: :class:`datetime.date`
    :returns: A datetime date object created from the string

    """
    return datetime.datetime.strptime(value, _YEAR_MONTH_DAY).date()


def _not_null(value, field):
    """Check whether 'value' should be coerced to 'field' type."""
    return value is not None or field.mode != 'NULLABLE'


def _int_from_json(value, field):
    """Coerce 'value' to an int, if set or not nullable."""
    if _not_null(value, field):
        return int(value)


def _float_from_json(value, field):
    """Coerce 'value' to a float, if set or not nullable."""
    if _not_null(value, field):
        return float(value)


def _decimal_from_json(value, field):
    """Coerce 'value' to a Decimal, if set or not nullable."""
    if _not_null(value, field):
        return decimal.Decimal(value)


def _bool_from_json(value, field):
    """Coerce 'value' to a bool, if set or not nullable."""
    if _not_null(value, field):
        return value.lower() in ['t', 'true', '1']


def _string_from_json(value, _):
    """NOOP string -> string coercion"""
    return value


def _bytes_from_json(value, field):
    """Base64-decode value"""
    if _not_null(value, field):
        return base64.standard_b64decode(value)


def _datetime_from_microseconds(value):
    """Convert timestamp to datetime, assuming UTC.

    :type value: float
    :param value: The timestamp to convert

    :rtype: :class:`datetime.datetime`
    :returns: The datetime object created from the value.
    """
    return _EPOCH + datetime.timedelta(microseconds=value)


def _timestamp_from_json(value, field):
    """Coerce 'value' to a datetime, if set or not nullable."""
    if _not_null(value, field):
        # value will be a float in seconds, to microsecond precision, in UTC.
        return _datetime_from_microseconds(1e6 * float(value))


def _datetime_from_json(value, field):
    """Coerce 'value' to a datetime, if set or not nullable.

    Args:
        value (str): The timestamp.
        field (.SchemaField): The field corresponding to the value.

    Returns:
        Optional[datetime.datetime]: The parsed datetime object from
        ``value`` if the ``field`` is not null (otherwise it is
        :data:`None`).
    """
    if _not_null(value, field):
        if '.' in value:
            # YYYY-MM-DDTHH:MM:SS.ffffff
            return datetime.datetime.strptime(value, _RFC3339_MICROS_NO_ZULU)
        # YYYY-MM-DDTHH:MM:SS
        return datetime.datetime.strptime(value, _RFC3339_NO_FRACTION)
    else:
        return None


def _date_from_json(value, field):
    """Coerce 'value' to a datetime date, if set or not nullable"""
    if _not_null(value, field):
        # value will be a string, in YYYY-MM-DD form.
        return _date_from_iso8601_date(value)


def _time_from_json(value, field):
    """Coerce 'value' to a datetime date, if set or not nullable"""
    if _not_null(value, field):
        # value will be a string, in HH:MM:SS form.
        return _time_from_iso8601_time_naive(value)


def _time_from_iso8601_time_naive(value):
    """Convert a zoneless ISO8601 time string to naive datetime time

    :type value: str
    :param value: The time string to convert

    :rtype: :class:`datetime.time`
    :returns: A datetime time object created from the string

    """
    return datetime.datetime.strptime(value, '%H:%M:%S').time()


def _record_from_json(value, field):
    """Coerce 'value' to a mapping, if set or not nullable."""
    if _not_null(value, field):
        record = {}
        record_iter = zip(field.fields, value['f'])
        for subfield, cell in record_iter:
            converter = _CELLDATA_FROM_JSON[subfield.field_type]
            if subfield.mode == 'REPEATED':
                value = [converter(item['v'], subfield) for item in cell['v']]
            else:
                value = converter(cell['v'], subfield)
            record[subfield.name] = value
        return record


_CELLDATA_FROM_JSON = {
    'INTEGER': _int_from_json,
    'INT64': _int_from_json,
    'FLOAT': _float_from_json,
    'FLOAT64': _float_from_json,
    'NUMERIC': _decimal_from_json,
    'BOOLEAN': _bool_from_json,
    'BOOL': _bool_from_json,
    'STRING': _string_from_json,
    'BYTES': _bytes_from_json,
    'TIMESTAMP': _timestamp_from_json,
    'DATETIME': _datetime_from_json,
    'DATE': _date_from_json,
    'TIME': _time_from_json,
    'RECORD': _record_from_json,
}


class SchemaField:
    """Class represent single field in schema"""

    def __init__(self, api_repr):
        self.name = api_repr['name']
        self.field_type = api_repr['type'].upper()
        self.mode = api_repr.get('mode', 'NULLABLE')
        self.description = api_repr.get('description')
        fields = api_repr.get('fields', ())

        self.fields = [SchemaField(f) for f in fields]


class Schema:
    """
    Build schema from query_job response.

    Google return query results in raw format.
    Part of response contain schema description for this data.
    All fields should be converted to proper python types defined by this schema.
    This class should parse and represent schema from api response.

    Reference: https://cloud.google.com/bigquery/docs/schemas
    """
    def __init__(self, api_response):
        self.schema = []

        for field in api_response['schema']['fields']:
            self.schema.append(SchemaField(field))


class BigQuerySchemaError(Exception):
    pass


class QueryJob:
    """
    Class that handle query job response
    """

    def __init__(self, response):
        self.job_id = response['jobReference']['jobId']
        self.page_token = response.get('pageToken')
        self.schema = Schema(response).schema

        self.rows = self.rows_builder(response.get('rows', list()))

    def rows_builder(self, values):
        """Convert JSON row data to rows with appropriate types."""
        return [self._row_dict_from_json(row) for row in values]

    def _row_dict_from_json(self, row):
        """Convert single row data to row with appropriate types.

            :type row: dict
            :param row: A JSON response row to be converted.

            :rtype: dict
            :returns: A dict with keys equal column name.
        """
        row_data = {}
        for field, cell in zip(self.schema, row['f']):
            if field.field_type not in _CELLDATA_FROM_JSON:
                raise BigQuerySchemaError('Unknown field type %s' % field.field_type)
            converter = _CELLDATA_FROM_JSON[field.field_type]
            if field.mode == 'REPEATED':
                row_data[field.name] = [converter(item['v'], field) for item in cell['v']]
            else:
                row_data[field.name] = converter(cell['v'], field)

        return row_data
