import logging
import sys
import unittest


class LogMyTestCase(type):
    def __new__(cls, name, bases, dct):
        # if the TestCase already provides setUp, wrap it
        if 'setUp' in dct:
            setUp = dct['setUp']
        else:
            setUp = lambda self: None

        def wrappedSetUp(self):
            # for hdlr in self.logger.handlers:
            #    self.logger.removeHandler(hdlr)
            self.hdlr = logging.StreamHandler(sys.stdout)
            self.logger.addHandler(self.hdlr)
            setUp(self)

        dct['setUp'] = wrappedSetUp

        # same for tearDown
        if 'tearDown' in dct:
            tearDown = dct['tearDown']
        else:
            tearDown = lambda self: None

        def wrappedTearDown(self):
            tearDown(self)
            self.logger.removeHandler(self.hdlr)

        dct['tearDown'] = wrappedTearDown

        # return the class instance with the replaced setUp/tearDown
        return type.__new__(cls, name, bases, dct)


class LoggedTestCase(unittest.TestCase):
    __metaclass__ = LogMyTestCase
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # or whatever you prefer
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)-8s %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
