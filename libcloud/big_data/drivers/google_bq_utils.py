
import datetime
import base64
import decimal
import calendar
import six

from pytz import UTC


DEFAULT_TASK_COMPLETION_TIMEOUT = 180
_RFC3339_MICROS_NO_ZULU = '%Y-%m-%dT%H:%M:%S.%f'
_RFC3339_NO_FRACTION = '%Y-%m-%dT%H:%M:%S'
_EPOCH = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=UTC)


def _millis_from_datetime(value):
    """Convert non-none datetime to timestamp, assuming UTC.

    :type value: :class:`datetime.datetime`
    :param value: (Optional) the timestamp

    :rtype: int, or ``NoneType``
    :returns: the timestamp, in milliseconds, or None
    """
    if value is not None:
        return _millis(value)


def _microseconds_from_datetime(value):
    """Convert non-none datetime to microseconds.

    :type value: :class:`datetime.datetime`
    :param value: The timestamp to convert.

    :rtype: int
    :returns: The timestamp, in microseconds.
    """
    if not value.tzinfo:
        value = value.replace(tzinfo=UTC)
    # Regardless of what timezone is on the value, convert it to UTC.
    value = value.astimezone(UTC)
    # Convert the datetime to a microsecond timestamp.
    return int(calendar.timegm(value.timetuple()) * 1e6) + value.microsecond


def _millis(when):
    """Convert a zone-aware datetime to integer milliseconds.

    :type when: :class:`datetime.datetime`
    :param when: the datetime to convert

    :rtype: int
    :returns: milliseconds since epoch for ``when``
    """
    micros = _microseconds_from_datetime(when)
    return micros // 1000


def _date_from_iso8601_date(value):
    """Convert a ISO8601 date string to native datetime date

    :type value: str
    :param value: The date string to convert

    :rtype: :class:`datetime.date`
    :returns: A datetime date object created from the string

    """
    return datetime.datetime.strptime(value, '%Y-%m-%d').date()


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
        return base64.standard_b64decode(_to_bytes(value))


def _datetime_from_microseconds(value):
    """Convert timestamp to datetime, assuming UTC.

    :type value: float
    :param value: The timestamp to convert

    :rtype: :class:`datetime.datetime`
    :returns: The datetime object created from the value.
    """
    return _EPOCH + datetime.timedelta(microseconds=value)


def _to_bytes(value, encoding='ascii'):
    """Converts a string value to bytes, if necessary.

    Unfortunately, ``six.b`` is insufficient for this task since in
    Python2 it does not modify ``unicode`` objects.

    :type value: str / bytes or unicode
    :param value: The string/bytes value to be converted.

    :type encoding: str
    :param encoding: The encoding to use to convert unicode to bytes. Defaults
                     to "ascii", which will not allow any characters from
                     ordinals larger than 127. Other useful values are
                     "latin-1", which which will only allows byte ordinals
                     (up to 255) and "utf-8", which will encode any unicode
                     that needs to be.

    :rtype: str / bytes
    :returns: The original value converted to bytes (if unicode) or as passed
              in if it started out as bytes.
    :raises TypeError: if the value could not be converted to bytes.
    """
    result = (value.encode(encoding)
              if isinstance(value, six.text_type) else value)
    if isinstance(result, six.binary_type):
        return result
    else:
        raise TypeError('%r could not be converted to bytes' % (value,))


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
        else:
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

    def __init__(self, raw):
        self.name = raw['name']
        self.field_type = raw['type']
        self.mode = raw.get('mode', 'NULLABLE')
        self.description = raw.get('description')


class Schema:
    """
    Build schema from query_job response
    """
    def __init__(self, schema_section):
        self.schema = []
        for field in schema_section['fields']:
            self.schema.append(SchemaField(field))

class QueryJob:
    """
    Class that handle query job response
    """

    def __init__(self, response):
        self.job_id = response['jobReference']['jobId']
        self.page_token = response.get('pageToken')
        self.schema = Schema(response['schema']).schema
        self.rows = self.rows_builder(response['rows'])


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
            converter = _CELLDATA_FROM_JSON[field.field_type]
            if field.mode == 'REPEATED':
                row_data[field.name] = [converter(item['v'], field) for item in cell['v']]
            else:
                row_data[field.name] = converter(cell['v'], field)

        return row_data
