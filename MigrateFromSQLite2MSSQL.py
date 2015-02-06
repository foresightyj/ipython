# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

from sqlalchemy import *
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Float, Enum

sqlite_engine = create_engine("sqlite:///urls_tested.db")
sqlite_engine.echo = False

Base = declarative_base()

class Url(Base):
    __tablename__ = "urls"
    id = Column(Integer, primary_key = True)
    url = Column(String(100), unique=True, nullable=False)# whether we are checking this url as the last possible page or as last+1 page.
    page_type = Column(Enum('LAST', 'BEYONDLAST', name='page_type'), nullable=False)
    status_code = Column(Integer, nullable=False)
    response_time = Column(Float, nullable=False)
    last_checked = Column(DateTime, nullable=False, default=datetime.utcnow)
    html = Column(Text, nullable=False)
        
    def __repr__(self):
        return "<Url(url='%s', page_type='%s', status_code='%d', last_checked='%s', response_time='%d')" % (self.url, self.page_type, self.status_code, self.last_checked, self.response_time)
    
Base.metadata.create_all(sqlite_engine)

# <codecell>

mssqlserver_engine = create_engine("mssql+pyodbc://yuanjian:xixinannan@localhost/urls_test", echo=False)
Base.metadata.create_all(mssqlserver_engine)

# <codecell>

from sqlalchemy.orm import sessionmaker
SQLiteSession = sessionmaker(bind=sqlite_engine)
sqlite_session = SQLiteSession()

# <codecell>

from sqlalchemy.orm import sessionmaker
MSSQLSession = sessionmaker(bind=mssqlserver_engine)
mssql_session = MSSQLSession()

# <markdowncell>

# # Naive one to one copy

# <codecell>

for url in sqlite_session.query(Url).all():
    copy_url = Url(url=url.url, status_code=url.status_code, response_time=url.response_time, html=url.html, page_type=url.page_type)
    mssql_session.add(copy_url)
    mssql_session.commit()

# <codecell>


