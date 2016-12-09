""" Utility functions
"""
import random
import time
import constants
import codecs
import os
import struct


guid_rng = random.Random()   # Uses urandom seed


def _collector_url_from_hostport(host, port):
    """
    Create an appropriate collector URL given the parameters.
    """
    return ''.join(['http://', host, ':', str(port), '/api/v1/spans'])


def _generate_guid():
    """
    Construct a guid - a random 64 bit integer
    """
    return guid_rng.getrandbits(64) - 1


def _id_to_hex(id):
    if id is None:
        return None
    return '{0:x}'.format(id)


def _now_micros():
    """
    Get the current time in microseconds since the epoch.
    """
    return _time_to_micros(time.time())


def _time_to_micros(t):
    """
    Convert a time.time()-style timestamp to microseconds.
    """
    return long(round(t * constants.SECONDS_TO_MICRO))


def _merge_dicts(*dict_args):
    """Destructively merges dictionaries, returns None
    instead of an empty dictionary.

    Elements of dict_args can be None.
    Keys in latter dicts override those in earlier ones.
    """
    result = {}
    for dictionary in dict_args:
        if dictionary:
            result.update(dictionary)
    return result if result else None


def _coerce_str(str_or_unicode):
    if isinstance(str_or_unicode, str):
        return str_or_unicode
    elif isinstance(str_or_unicode, unicode):
        return str_or_unicode.encode('utf-8', 'replace')
    else:
        try:
            return str(str_or_unicode)
        except Exception:
            # Never let these errors bubble up
            return '(encoding error)'


def generate_random_64bit_string():
    """Returns a 64 bit UTF-8 encoded string. In the interests of simplicity,
    this is always cast to a `str` instead of (in py2 land) a unicode string.
    Certain clients (I'm looking at you, Twisted) don't enjoy unicode headers.
    :returns: random 16-character string
    """
    return str(codecs.encode(os.urandom(8), 'hex_codec').decode('utf-8'))


def unsigned_hex_to_signed_int(hex_string):
    """Converts a 64-bit hex string to a signed int value.
    This is due to the fact that Apache Thrift only has signed values.
    Examples:
        '17133d482ba4f605' => 1662740067609015813
        'b6dbb1c2b362bf51' => -5270423489115668655
    :param hex_string: the string representation of a zipkin ID
    :returns: signed int representation
    """
    return struct.unpack('q', struct.pack('Q', int(hex_string, 16)))[0]


def signed_int_to_unsigned_hex(signed_int):
    """Converts a signed int value to a 64-bit hex string.
    Examples:
        1662740067609015813  => '17133d482ba4f605'
        -5270423489115668655 => 'b6dbb1c2b362bf51'
    :param signed_int: an int to convert
    :returns: unsigned hex string
    """
    hex_string = hex(struct.unpack('Q', struct.pack('q', signed_int))[0])[2:]
    if hex_string.endswith('L'):
        return hex_string[:-1]
    return hex_string
