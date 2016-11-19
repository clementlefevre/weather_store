# encoding: utf-8

"""
Persists weather data into a db
"""

from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey
from sqlalchemy import UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import IntegrityError, OperationalError

import psycopg2

import settings


psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)


DeclarativeBase = declarative_base()


class DB_tool(object):
    """Livingsocial pipeline for storing scraped items in the database"""

    def __init__(self):
        """Initializes database connection and sessionmaker.

        Creates all table.

        """
        engine = db_connect()
        create_table(engine)
        self.Session = sessionmaker(bind=engine)

    def get_sites(self):
        session = self.Session()
        return session.query(Site).all()

    def delete_weather_data(self):
        session = self.Session()
        session.query(WeatherData).delete()

    def get_weather_data(self):
        session = self.Session()
        return session.query(WeatherData).all()

    def store_weather_data(self, weather_data):

        try:
            engine = db_connect()
            self.Session = sessionmaker(bind=engine)
            session = self.Session()
            result = session.execute("select 'OK'")  # and retry

            # session = self.Session()
            # result = session.execute("select 'OK'")

        except OperationalError as error:
            print "oupssss"
            print error.message
            engine = db_connect()
            self.Session = sessionmaker(bind=engine)
            session = self.Session()
            result = session.execute("select 'OK'")  # and retry

        try:
            session.add(weather_data)
            session.commit()

        except IntegrityError as ex:

            session.rollback()
        except Exception as ex:
            template = "An exception of type {0} occured. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print message
            session.rollback()
            raise
        finally:
            session.close()
            engine.dispose()


def db_connect():
    """Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance.

    """
    return create_engine(URL(**settings.DATABASE))


def create_table(engine):
    DeclarativeBase.metadata.create_all(engine, checkfirst=True)


class Site(DeclarativeBase):

    __tablename__ = "sites"

    id = Column(Integer, primary_key=True)
    idbldsite = Column('idbldsite', Integer)
    sname = Column('sname', String, nullable=True)
    timezone = Column('timezone', String, nullable=False)
    latitude = Column('latitude', Integer, nullable=False)
    longitude = Column('longitude', Integer, nullable=False)
    customer = Column('customer', String, nullable=False)
    coordinates = Column('coordinates', String, nullable=False)
    weather_data = relationship("WeatherData")

    def __repr__(self):
        return self.sname.encode("utf8") + "#" + self.customer.encode('utf8') + "#" + str(self.coordinates)


class WeatherData(DeclarativeBase):
    __tablename__ = "weather_data"

    id = Column(Integer, primary_key=True)
    period = Column('period', String, nullable=False)
    data_type = Column('data_type', String, nullable=False)
    updated = Column('updated', DateTime, nullable=False)
    dateTime = Column('dateTime', DateTime, nullable=False)
    tt = Column("tt", Integer, nullable=True)
    tn = Column("tn", Integer, nullable=True)
    tx = Column("tx", Integer, nullable=True)
    ne = Column("ne", Integer, nullable=True)
    ww = Column("ww", Integer, nullable=True)
    rrr = Column("rrr", Integer, nullable=True)
    prrr = Column("prrr", Integer, nullable=True)
    site_id = Column(Integer, ForeignKey('sites.id'))

    __table_args__ = (UniqueConstraint(
        'site_id', 'updated', 'dateTime', 'data_type', name='_update_time_uc'),)
