from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import engine_from_config, TypeDecorator, Index, Column, BigInteger, Integer, String, Boolean, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, backref, scoped_session
from sqlalchemy.inspection import inspect
from datetime import datetime, timedelta
import ast


with open('config.py') as f:
    engine = engine_from_config(ast.literal_eval(f.read()))
inspector = inspect(engine)
Base = declarative_base(bind=engine)
Session = scoped_session(sessionmaker(bind=engine))


class MicrosecondTimestamp(TypeDecorator):
    impl = BigInteger
    epoch = datetime(1970, 1, 1)
    def process_bind_param(self, value, dialect):
        return long((value - self.epoch).total_seconds() * 10 ** 6)
    def process_result_value(self, value, dialect):
        return self.epoch + timedelta(microseconds=value)


class CameraInfo(Base):
    __tablename__ = 'CameraInfo'
    __table_args__ = (
            Index('creationTimeStamp_serialNumber', 'creationTimeStamp', 'serialNumber', unique=True),
            {'mysql_engine':'InnoDB'},
            )
    
    id = Column(Integer, primary_key=True)
    creationTimeStamp = Column(MicrosecondTimestamp)
    serialNumber = Column(Integer)
    
    applicationIPAddress = Column(BigInteger)
    applicationPort = Column(Integer)
    bayerTileFormat = Column(Integer)
    busNumber = Column(Integer)
    ccpStatus = Column(Integer)
    driverName = Column(String(255))
    driverType = Column(Integer)
    firmwareBuildTime = Column(String(255))
    firmwareVersion = Column(String(255))
    gigEMajorVersion = Column(Integer)
    gigEMinorVersion = Column(Integer)
    iidcVer = Column(Integer)
    interfaceType = Column(Integer)
    isColorCamera = Column(Boolean)
    maximumBusSpeed = Column(Integer)
    modelName = Column(String(255))
    nodeNumber = Column(Integer)
    pcieBusSpeed = Column(Integer)
    sensorInfo = Column(String(255))
    sensorResolution = Column(String(255))
    userDefinedName = Column(String(255))
    vendorName = Column(String(255))
    xmlURL1 = Column(String(255))
    xmlURL2 = Column(String(255))
    
    
class FC2Version(Base):
    __tablename__ = 'FC2Version'
    __table_args__ = (
            Index('creationTimeStamp_serialNumber', 'creationTimeStamp', 'serialNumber', unique=True),
            {'mysql_engine':'InnoDB'},
            )
    
    id = Column(Integer, primary_key=True)
    creationTimeStamp = Column(MicrosecondTimestamp)
    serialNumber = Column(Integer)
    
    major = Column(Integer)
    minor = Column(Integer)
    type = Column(Integer)
    build = Column(Integer)
    
class SystemInfo(Base):
    __tablename__ = 'SystemInfo'
    __table_args__ = (
            Index('creationTimeStamp_serialNumber', 'creationTimeStamp', 'serialNumber', unique=True),
            {'mysql_engine':'InnoDB'},
            )
    
    id = Column(Integer, primary_key=True)
    creationTimeStamp = Column(MicrosecondTimestamp)
    serialNumber = Column(Integer)
    
    osType = Column(Integer)
    osDescription = Column(String(255))
    byteOrder = Column(Integer)
    sysMemSize = Column(Integer)
    cpuDescription = Column(String(255))
    numCpuCores = Column(Integer)
    driverList = Column(String(255))
    libraryList = Column(String(255))
    gpuDescription = Column(String(255))
    screenWidth = Column(Integer)
    screenHeight = Column(Integer)


class ImageMetadata(Base):
    __tablename__ = 'ImageMetadata'
    __table_args__ = (
            Index('creationTimeStamp_serialNumber', 'creationTimeStamp', 'serialNumber', unique=True),
            {'mysql_engine':'InnoDB'},
            )
    
    id = Column(Integer, primary_key=True)
    creationTimeStamp = Column(MicrosecondTimestamp)
    serialNumber = Column(Integer)
    
    frameNumber = Column(Integer)
    embeddedTimeStamp = Column(BigInteger)
    embeddedGain = Column(Integer)
    embeddedShutter = Column(Integer)
    embeddedBrightness = Column(Integer)
    embeddedExposure = Column(Integer)
    embeddedWhiteBalance = Column(Integer)
    embeddedFrameCounter = Column(Integer)
    embeddedStrobePattern = Column(Integer)
    embeddedGPIOPinState = Column(Integer)
    embeddedROIPosition = Column(Integer)
    
    CameraInfo_id = Column(Integer, ForeignKey(CameraInfo.id))
    FC2Version_id = Column(Integer, ForeignKey(FC2Version.id))
    SystemInfo_id = Column(Integer, ForeignKey(SystemInfo.id))
    
    CameraInfo = relationship(CameraInfo, backref=backref('ImageMetadatas'))
    FC2Version = relationship(FC2Version, backref=backref('ImageMetadatas'))
    SystemInfo = relationship(SystemInfo, backref=backref('ImageMetadatas'))
    
    
def recreate():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    
    