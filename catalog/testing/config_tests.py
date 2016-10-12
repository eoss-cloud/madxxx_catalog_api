import unittest

from api import serialize
from api.eoss_api import Api


class ApiTest(unittest.TestCase):
    """Tests ConfigManager and local/remote config access."""

    def testCreateConfig(self):
        """
        Create simple config object from string
        """
        api = Api()
        serialize(api.catalog_get('LC80460242016241LGN00'))
        serialize(api.catalog_get('S2A_OPER_PRD_MSIL1C_PDMC_20160806T041636_R131_V20151020T013517_20151020T013517'))
        api.catalog_delete('LC80460152016241LGN00')
        serialize(api.catalog_get('LC80460152016241LGN00'))


if __name__ == '__main__':
    """
    Perform all tests
    """
    unittest.main()
