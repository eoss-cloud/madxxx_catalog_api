# Created by sgebhardt at 06.10.16
# Copyright EOSS GmbH 2016
import unittest
from datetime import datetime, timedelta

from manage.eosscatalog import EOSSCatalog
from manage.landsat_catalog import USGSCatalog
from manage.sentinelcatalog import SentinelCatalog
from manage.urthecastcatalog import UrthecastCatalog
from pytz import UTC


class UsgsCatalogTest(unittest.TestCase):
    """Tests USGS catalog access"""

    def testCatalog(self):
        """
        Create simple config object from string
        """
        sensor = 'LANDSAT_8'
        cloud_ratio = 1.0
        ag_season_start = datetime(2016, 3, 1, tzinfo=UTC)
        ag_season_end = datetime(2016, 4, 15, tzinfo=UTC)
        aoi_nw = (-92, 19)
        aoi_se = (-91, 18)
        aoi_ne = (aoi_se[0], aoi_nw[1])
        aoi_sw = (aoi_nw[0], aoi_se[1])
        aoi = [aoi_nw, aoi_ne, aoi_se, aoi_sw, aoi_nw]

        cat = USGSCatalog()
        self.assertTrue(len(cat.find(sensor, aoi, ag_season_start, ag_season_end, cloud_ratio)) == 3)


class SentinelCatalogTest(unittest.TestCase):
    """
    Test copernicus service for sentinel images
    """

    def testCatalog(self):
        provider = 'sentinel2'  # sentinel1, sentinel2
        max_cloud_ratio = 0.4
        max_black_fill = 0.1
        ag_season_start = datetime(2015, 1, 1, tzinfo=UTC)
        ag_season_end = datetime(2016, 12, 15, tzinfo=UTC)
        aoi_nw = (-91.5175095, 16.8333384)
        aoi_se = (-91.3617268, 16.8135385)
        aoi_ne = (aoi_se[0], aoi_nw[1])
        aoi_sw = (aoi_nw[0], aoi_se[1])
        aoi = [aoi_nw, aoi_ne, aoi_se, aoi_sw, aoi_nw]

        cat = SentinelCatalog()
        datasets = cat.find(provider, aoi, ag_season_start, ag_season_end)
        self.assertTrue(len(datasets) == 29)


class CompareCatalogTest(unittest.TestCase):
    def testComparitionStatic(self):
        ag_season_start = datetime(2016, 6, 2, tzinfo=UTC)
        ag_season_end = datetime(2016, 10, 6, tzinfo=UTC)
        aoi_nw = (-94.21561717987059, 35.26342169967158)
        aoi_se = (-94.21304225921631, 35.265278832862336)

        aoi_ne = (aoi_se[0], aoi_nw[1])
        aoi_sw = (aoi_nw[0], aoi_se[1])
        aoi = [aoi_nw, aoi_ne, aoi_se, aoi_sw, aoi_nw]

        cat = SentinelCatalog()
        datasets = cat.find('sentinel2', aoi, ag_season_start, ag_season_end)
        self.assertTrue(len(datasets) == 7)

        cat = EOSSCatalog()
        datasets = cat.find('Sentinel2', aoi, ag_season_start, ag_season_end, 1)
        self.assertTrue(len(datasets) == 7)

        cat = UrthecastCatalog()
        datasets = cat.find(['landsat-8'], aoi, ag_season_start, ag_season_end, cloud_ratio=1)
        self.assertTrue(len(datasets) == 3)

        cat = EOSSCatalog()
        datasets = cat.find('LANDSAT8', aoi, ag_season_start, ag_season_end, 1)
        self.assertTrue(len(datasets) == 11)

    def testComparitionDynamic(self):
        import random
        LS_YEARS = [2014, 2015, 2016]
        for x in range(10):
            year = random.choice(LS_YEARS)
            month = random.randint(1, 12)
            day = random.randint(1, 28)

            ag_season_start = datetime(year, month, day, tzinfo=UTC)
            ag_season_end = ag_season_start + timedelta(days=10)
            aoi_nw = (10, 52)
            aoi_se = (8, 50)

            aoi_ne = (aoi_se[0], aoi_nw[1])
            aoi_sw = (aoi_nw[0], aoi_se[1])
            aoi = [aoi_nw, aoi_ne, aoi_se, aoi_sw, aoi_nw]

            cat = EOSSCatalog()
            datasets = cat.find('Sentinel2', aoi, ag_season_start, ag_season_end, 1)
            self.assertTrue(len(datasets) >= 0)

            results = dict()
            for catalog, sensor in [(EOSSCatalog, 'LANDSAT8'), (UrthecastCatalog, 'landsat-8')]:
                cat = catalog()
                datasets = cat.find(sensor, aoi, ag_season_start, ag_season_end, cloud_ratio=1)
                results[catalog] = len(datasets)
                self.assertTrue(len(datasets) >= 0)
            print results
            self.assertGreaterEqual(results[EOSSCatalog], results[UrthecastCatalog])


if __name__ == '__main__':
    """
    Perform all tests
    """
    unittest.main()
