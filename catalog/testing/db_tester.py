import unittest
from sqlalchemy import Column, UniqueConstraint, String, Integer

from model import Context
import time
import random
import string


O = 1000
N = 100
M = 4

class Test_Table(Context().getBase()):
    __tablename__ = "tester1"
    __table_args__ = ( {'sqlite_autoincrement': True, 'schema': 'tmp'}
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String, nullable=False)
    val = Column(String, nullable=False)


class DatabaseTest(unittest.TestCase):
    """Tests ConfigManager and local/remote config access."""

    def setUp(self):
        self.session = Context().getSession()

    def tearDown(self):
        all_data = self.session.query(Test_Table).delete()
        self.session.commit()
        Context().closeSession()

    def testCreateData(self):
        for x in range(O):
            obj = Test_Table(key=''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(M)),
                                         val=''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(N)))
            try:
                self.session.add(obj)
                self.session.commit()
            except:
                self.session.rollback()
                #raise
            finally:
                self.session.close()
                Context().closeSession()

        self.assertEqual(self.session.query(Test_Table).count(), O)
        Context().closeSession()



if __name__ == '__main__':
    """
    Perform all tests
    """
    unittest.main()
