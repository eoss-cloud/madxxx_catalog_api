import cStringIO
import datetime
import gzip
import ujson
import urlparse

VERSION = "v1"
COMPLEX_TYPES = ["datetime", "Polygon", 'WKBElement']
DEF_EPSG = 4326


def can_zip_response(headers):
    """
    Check if request supports zipped response
    :param headers: request headers
    :return:
    """
    if 'ACCEPT-ENCODING' in headers.keys():
        if 'gzip' in headers.get('ACCEPT-ENCODING'):
            return True

    return False


def compress_body(body, level=9):
    """
    Compress request body as gzip
    :param body: string
    :param level: zip compression level
    :return: binary zipped body
    """
    zbuf = cStringIO.StringIO()
    zfile = gzip.GzipFile(mode='wb', fileobj=zbuf, compresslevel=level)
    zfile.write(body)
    zfile.close()

    return zbuf.getvalue()


def make_GeoJson(geoms, attrs):
    """
    Creates GeoJson structure with given geom and attribute lists; throws exception if both lists have different length
    :param geoms: list of geometries (needs to be encoded as geojson features)
    :param attrs: list of attributes
    :return: dict in GeoJson structure
    """
    assert len(geoms) == len(attrs), "lengths of geoms and attrs are different (%d/%d)" % (len(geoms), len(attrs))
    geojson_structure = dict()

    type = 'FeatureCollection'
    geojson_structure['type'] = type
    geojson_structure['features'] = list()
    geojson_structure["crs"] = {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}}

    counter = 0
    for g, a in zip(geoms, attrs):
        counter += 1
        feat = dict()
        feat["type"] = 'Feature'

        if not "gid" in a.keys():
            a['gid'] = counter
        feat["properties"] = a
        feat["geometry"] = g

        geojson_structure['features'].append(feat)
    return geojson_structure


def get_base_url(url):
    """
    Return base url from any given url
    e.g. http://services.eoss.cloud:8000 from
    http://services.eoss.cloud:8000/dataset/S2A_OPER_PRD_MSIL1C_PDMC_20160816T225502_R008_V20160816T104022_20160816T104025.json
    :param url: string
    :return:
    """
    url_struct = urlparse.urlparse(url)
    return '%s://%s' % (url_struct.scheme, url_struct.netloc)


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


# TODO: support lists and nested objects
def serialize(obj, as_json=True):
    """
    Encode simple object with its attributes into json string used for web interchange
    :param obj: simple python objects
    :return: json structure encoded as string
    """
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

    # print result
    if as_json:
        return ujson.dumps(result)
    else:
        return result
