# Created by sgebhardt at 16.08.16
# Copyright EOSS GmbH 2016

import gevent
import gevent.monkey
gevent.monkey.patch_all()


from sqlalchemy import create_engine
from sqlalchemy.ext.declarative.api import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.scoping import scoped_session
import sys, pwd, socket, os

from sqlalchemy.pool import QueuePool, NullPool

from utilities import read_OS_var


class Context(object):
    '''
    Singelton context of a running instance providing general information / configuration
    '''

    configuration = None
    base = declarative_base()
    version = "$EOSS_PROJECT_ID$"
    orm_initialized = None

    def __init__(self, url=None):
        '''
        Singelton context of a running instance providing general information / configuration
        '''

        self.version = 'v1'
        if url == None:
            self.url = read_OS_var("EOSS_CATALOG_DB", mandatory=False)
        else:
            self.url = url

        if self.orm_initialized is None:
            assert self.url != None, 'Please specify DB connection with EOSS_CATALOG_DB'
            self.init_orm()

    def __str__(self):
        return "<EOSS context version:%s>" % (self.version)

    def init_orm(self):
        """
        Initualize ORM component and create tables if necessary.
        :param url: sqlalchemy compatible DB url string
        :return: None - set sqlalchemy session as class attribute
        """

        prog = os.path.basename(sys.argv[0]) or 'eoss-api'
        username = pwd.getpwuid(os.getuid()).pw_name
        hostname = socket.gethostname().split(".")[0]

        self.engine = create_engine(self.url,
                                #    pool_size=10,
                                #    max_overflow=5,
                                #    pool_recycle=600,
                                    connect_args = {'application_name': "%s:%s@%s" %(prog, username, hostname)},
                                    poolclass=NullPool)
        self.getBase().metadata.create_all(self.engine)
        # metadata = schema.MetaData()

        self.session_factory = sessionmaker(bind=self.engine,
                                            autocommit=False,
                                            autoflush=False,
                                            expire_on_commit=True)
        self.session = scoped_session(self.session_factory)

        self.base.metadata.create_all(self.engine)
        self.orm_initialized = True

    def getSession(self):
        '''
        Return sqlalchemy session (http://docs.sqlalchemy.org/en/latest/orm/session_api.html)
        :return:
        '''
        return self.session()

    def closeSession(self):
        self.session.remove()

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
