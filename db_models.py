# coding: utf-8
from sqlalchemy import Column, Date, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata

class FileStatu(Base):
    __tablename__ = 'file_status'

    id = Column(Integer, primary_key=True)
    file_name = Column(String(32))
    stream_id = Column(ForeignKey(u'stream.id'), nullable=False, index=True)
    ref_date = Column(Date)
    record_count = Column(Integer)
    generate_session_id = Column(ForeignKey(u'generate_session.id'), nullable=False, index=True)
    load_session_id = Column(ForeignKey(u'load_session.id'), nullable=False, index=True)
    size = Column(Integer)
    archived = Column(Integer)

    generate_session = relationship(u'GenerateSession')
    load_session = relationship(u'LoadSession')
    stream = relationship(u'Stream')


class FtpTable(Base):
    __tablename__ = 'ftp_table'

    id = Column(Integer, primary_key=True)
    stream_id = Column(ForeignKey(u'stream.id'), nullable=False, index=True)
    ftp_type = Column(String(32))
    ip = Column(String(32))
    username = Column(String(32))
    password = Column(String(32))
    remote_dir = Column(String(100))
    local_dir = Column(String(100))
    search_query = Column(String(32))

    stream = relationship(u'Stream')


class GenerateHistory(Base):
    __tablename__ = 'generate_history'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    stage = Column(String(200))
    generate_session_id = Column(ForeignKey(u'generate_session.id'), nullable=False, index=True)

    generate_session = relationship(u'GenerateSession')


class GenerateSession(Base):
    __tablename__ = 'generate_session'

    id = Column(Integer, primary_key=True)
    ref_date = Column(Date)
    start_time = Column(DateTime)
    status = Column(String(32))
    end_time = Column(DateTime)
    duration = Column(Integer)
    stream_id = Column(ForeignKey(u'stream.id'), nullable=False, index=True)

    stream = relationship(u'Stream', backref=backref('generate_session', order_by=id))
    
    def startGenSession(self, ref_date, stream_id): 
        
        newGenSession= GenerateSession (ref_date=ref_date, start_time = dt.datetime.now(), status = "STARTING", stream_id = stream_id)
        session.add(newGenSession)
        session.commit()
        self.genSession=newGenSession
        

class GenerateThreadControl(Base):
    __tablename__ = 'generate_thread_control'

    id = Column(Integer, primary_key=True)
    stream_id = Column(ForeignKey(u'stream.id'), nullable=False, index=True)
    thread = Column(Integer)
    status = Column(String(32))
    message = Column(String(200))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    duration = Column(Integer)

    stream = relationship(u'Stream')


class LoadSession(Base):
    __tablename__ = 'load_session'

    id = Column(Integer, primary_key=True)
    stream_id = Column(ForeignKey(u'stream.id'), nullable=False, index=True)
    ref_date = Column(Date)
    start_time = Column(DateTime)
    status = Column(String(32))
    end_time = Column(DateTime)
    duration = Column(Integer)

    stream = relationship(u'Stream')


class LoadThreadControl(Base):
    __tablename__ = 'load_thread_control'

    id = Column(Integer, primary_key=True)
    stream_id = Column(ForeignKey(u'stream.id'), nullable=False, index=True)
    thread = Column(Integer)
    status = Column(String(32))
    message = Column(String(200))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    duration = Column(Integer)

    stream = relationship(u'Stream')


class LoadingHistory(Base):
    __tablename__ = 'loading_history'

    id = Column(Integer, primary_key=True)
    load_session_id = Column(ForeignKey(u'load_session.id'), nullable=False, index=True)
    timestamp = Column(DateTime)
    stage = Column(String(200))

    load_session = relationship(u'LoadSession')


class Stream(Base):
    __tablename__ = 'stream'

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
    sample_filename = Column(String(45))
    in_dir = Column(String(100))
    compressed = Column(Integer)
    ftp = Column(Integer)
    files_per_session = Column(Integer)
    no_threads= Column(Integer)
