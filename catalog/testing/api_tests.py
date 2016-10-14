import unittest

from dateutil.parser import parse

from api.eoss_api import Api


class ApiTest(unittest.TestCase):
    """Tests ConfigManager and local/remote config access."""

    def setUp(self):
        self.api = Api()  # url='http://api.eoss.cloud'

    def testCreateConfig(self):
        """
        Create simple config object from string
        """

        ds = self.api.get_dataset('LC81920272016240LGN00')
        self.assertEqual(len(ds), 1)

        ds = self.api.get_dataset('LE71010172003151EDC00')
        self.assertEqual(len(ds), 1)

        ds = self.api.get_dataset('S2A_OPER_PRD_MSIL1C_PDMC_20160806T202847_R142_V20160805T192909_20160805T192909')
        self.assertEqual(len(ds), 21)

    def testCatalogSearch(self):
        aoi_nw = (-91.5175095, 16.8333384)
        aoi_se = (-91.3617268, 16.8135385)
        aoi_ne = (aoi_se[0], aoi_nw[1])
        aoi_sw = (aoi_nw[0], aoi_se[1])
        aoi = [aoi_nw, aoi_ne, aoi_se, aoi_sw, aoi_nw]

        # Object representation
        results = self.api.search_dataset(aoi, 100, parse('2015-01-01'), parse('2015-03-01'), 'landsat8', full_objects=True)
        self.assertEqual(len(results), 3)
        for item in results:
            self.assertTrue(type(item).__name__ == 'Catalog_Dataset')

        results = self.api.search_dataset(aoi, 100, parse('2015-01-01'), parse('2015-03-01'), 'landsat8', full_objects=False)
        # JSON representation
        self.assertEqual(len(results), 3)
        for item in results:
            self.assertTrue(type(item) == dict)


if __name__ == '__main__':
    """
    Perform all tests
    """
    unittest.main()
