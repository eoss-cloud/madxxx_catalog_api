import datetime
import json

import dateutil.parser
import falcon

VERSION = "v1"
COMPLEX_TYPES = ["datetime", "Polygon", 'WKBElement']
DEF_EPSG = 4326


def get_class_members(cls):
    """
    Inspect class
    :param cls:
    :return:
    """
    ret = dir(cls)
    if hasattr(cls, '__bases__'):
        for base in cls.__bases__:
            ret = ret + get_class_members(base)
    return ret


def get_object_attrs(obj):
    """
    Extract all attributes of an object and skip class methods
    :param obj:
    :return:
    """
    ret = dir(obj)
    attributes = set()
    for x in ret:
        if "__" not in str(x):
            # print type(x), x
            attributes.add(x)

    if hasattr(obj, '__class__'):
        ret.append('__class__')
        ret.extend(get_class_members(obj.__class__))
        ret = list(set(ret))

    return attributes


class General_Structure(object):
    """
    Abstract class used for object creation
    """

    def __init__(self, entries, types):
        """

        :param entries: k/v for attributes
        :param types:  k/v for attribute types to support special var type handling
        """
        for k, v in entries.iteritems():
            # print types[k], k
            if types[k] not in COMPLEX_TYPES:
                setattr(self, k, v)
            elif types[k] == "datetime":
                setattr(self, k, dateutil.parser.parse(v))
            elif 'WKBElement' in types[k]:
                print 'WKBElement'
            else:
                raise Exception("Key:%s (%s) not supported in General_Structure - will not be skipped" % (k, types[k]))

    def __iter__(self):
        # remove 'private' attributes from object
        for x, y in self.__dict__.items():
            if not "__" in x[:2]:
                yield x, y


# TODO: support lists and nested objects
def deserialize(json_structure, is_string=True):
    """
    Create object based on json structure
    :param json_structure:  json structure of object decomposition
    :param is_string: True if json_structure is still string encoded
    :return: reconstructed object
    """
    if is_string:
        obj_structure = json.loads(json_structure)
    else:
        obj_structure = json_structure

    obj = General_Structure(obj_structure["data"], obj_structure["types"])
    obj.__class__.__name__ = str(obj_structure["class-name"])
    obj.__composed_at__ = str(datetime.datetime.now().replace(microsecond=0).isoformat())
    obj.__composition__ = "(De-)Serializtation:%s" % VERSION

    return obj


def convert_obj(obj):
    result = dict()
    result["class-name"] = obj.__class__.__name__

    result["data"] = dict()
    result["types"] = dict()
    result["created"] = str(datetime.datetime.now().replace(microsecond=0).isoformat())
    result["serializer"] = "(De-)Serializtation:%s" % VERSION
    for x in get_object_attrs(obj):
        var_type = type(getattr(obj, x)).__name__
        if var_type not in COMPLEX_TYPES:
            result["data"][x] = getattr(obj, x)
        elif var_type == "datetime":
            result["data"][x] = str(getattr(obj, x).isoformat())
        elif var_type == "Polygon":
            result["data"][x] = 'SRID=%d;' % DEF_EPSG + getattr(obj, x).wkt
            var_type = 'str'
        else:
            raise Exception("Key:%s (%s) not supported in serialization - will not be skipped" % (x, var_type))
        result["types"][x] = var_type

    return result


def serialize(obj, as_json=True):
    """
    Encode simple object with its attributes into json string used for web interchange
    :param obj: simple python objects
    :return: json structure encoded as string
    """
    if isinstance(obj, General_Structure):
        result = convert_obj(obj)
    elif type(obj) is list:
        result = list()
        for o in obj:
            result.append(convert_obj(o))

    # print result
    if as_json:
        return json.dumps(result)
    else:
        return result


def max_body(limit):
    """
    Hook to check max. request body size
    :param limit: limit in bytes
    :return: throws exception if body is to large
    """

    def hook(req, resp, resource, params):
        length = req.content_length
        if length is not None and length > limit:
            msg = ('The size of the request is too large. The body must not '
                   'exceed ' + str(limit) + ' bytes in length.')

            raise falcon.HTTPRequestEntityTooLarge(
                'Request body is too large', msg)

    return hook
