import datetime
import unittest

from api import serialize, deserialize


class Test(object):
    a = 1
    b = "a"

    def __init__(self):
        self.x = 1


class Serialization(unittest.TestCase):
    def test_types(self):
        x = Test()
        x.xx = datetime.datetime.now()
        x.yy = 1.1123

        y = deserialize(serialize(x))
        self.assertEquals(x.xx, y.xx)
        self.assertEquals(x.a, y.a)
        self.assertEquals(x.b, y.b)
        self.assertEquals(x.yy, y.yy)
