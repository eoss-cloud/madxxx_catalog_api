# Created by sgebhardt at 16.08.16
# Copyright EOSS GmbH 2016

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative.api import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.scoping import scoped_session

from utilities import read_OS_var


class Context(object):
    '''
    Singelton context of a running instance providing general information / configuration
    '''

    configuration = None
    base = declarative_base()
    version = "$EOSS_PROJECT_ID$"

    def __init__(self, url=None, orm_initialized=False):
        '''
        Singelton context of a running instance providing general information / configuration
        '''

        self.version = 'v1'
        if url == None:
            self.url = read_OS_var("EOSS_CATALOG_DB", mandatory=False)
        else:
            self.url = url

        if not orm_initialized:
            self.init_orm()

    def __str__(self):
        return "<EOSS context v:%d>" % (self.version)

    def init_orm(self):
        """
        Initualize ORM component and create tables if necessary.
        :param url: sqlalchemy compatible DB url string
        :return: None - set sqlalchemy session as class attribute
        """
        assert self.url != None, 'Please specify DB connection with EOSS_CATALOG_DB'
        self.engine = create_engine(self.url)
        self.getBase().metadata.create_all(self.engine)
        # metadata = schema.MetaData()

        self.session_factory = sessionmaker(bind=self.engine)
        self.session = scoped_session(self.session_factory)

        self.base.metadata.create_all(self.engine)
        self.orm_initialized = True

    def getSession(self):
        '''
        Return sqlalchemy session (http://docs.sqlalchemy.org/en/latest/orm/session_api.html)
        :return:
        '''
        return self.session()

    def get_engine(self):
        """
        Get DB engine from sqlalchemy for direct manipulation
        (http://docs.sqlalchemy.org/en/latest/core/connections.html?highlight=engine#module-sqlalchemy.engine)
        :return: sqlalchemy db engine
        """
        return self.engine

    def getBase(self):
        """
        Return sqlalchemy base necessary for table registrations (http://docs.sqlalchemy.org/en/latest/orm/extensions/declarative/api.html)
        :return:
        """
        return self.base

    def __new__(cls, *attr, **kwargs):
        """ Handle Context object as singelton"""

        if not hasattr(cls, '_inst'):
            cls._inst = super(Context, cls).__new__(cls, *attr, **kwargs)
        return cls._inst
