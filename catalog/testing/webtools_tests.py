import unittest

from testing.logged_unittest import LoggedTestCase
from utilities.web_utils import remote_file_exists, public_key_exists, public_get_filestream


class WebResourcesTest(LoggedTestCase):
    def testExists(self):
        self.assertFalse(remote_file_exists('http://foo/bar.txt'))
        self.assertTrue(remote_file_exists('http://www.google.com'))


class S3Test(LoggedTestCase):
    def testPublicExists(self):
        self.assertTrue(public_key_exists('sentinel-s2-l1c', 'tiles/44/W/PS/2016/8/15/1/preview.jpg'))

    def testPublicExistsFalse(self):
        self.assertFalse(public_key_exists('sentinel-s2-l1c', 'tiles/44/W/PS/2016/8/15/1/preview'))

    def testPublicRead(self):
        self.assertTrue(len(public_get_filestream('sentinel-s2-l1c', 'tiles/44/W/PS/2016/8/15/1/preview.jpg')) == 3283)
        self.assertIsNone(public_get_filestream('sentinel-s2-l1c', 'tiles/44/W/PS/2016/8/15/1/preview'))


if __name__ == '__main__':
    """
    Perform all tests
    """
    unittest.main()
