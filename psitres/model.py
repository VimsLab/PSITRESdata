from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Index, Column, Integer, DateTime, String, Boolean, create_engine, inspect, ForeignKey, UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship, backref

engine = create_engine('sqlite:////home/rhein/psitres/psitres_data/test.sqlite', echo=False)
Base = declarative_base(bind=engine)
Session = sessionmaker(bind=engine)

class CameraInfo(Base):
    __tablename__ = 'CameraInfo'
    __table_args__ = (
            UniqueConstraint('creationTimeStamp', 'serialNumber'),
            Index('CameraInfoIndex', 'creationTimeStamp', 'serialNumber', unique=True)
            )
    
    id = Column(Integer, primary_key=True)
    creationTimeStamp = Column(DateTime)
    serialNumber = Column(Integer)
    
    applicationIPAddress = Column(Integer)
    applicationPort = Column(Integer)
    bayerTileFormat = Column(Integer)
    busNumber = Column(Integer)
    ccpStatus = Column(Integer)
    driverName = Column(String)
    driverType = Column(Integer)
    firmwareBuildTime = Column(String)
    firmwareVersion = Column(String)
    gigEMajorVersion = Column(Integer)
    gigEMinorVersion = Column(Integer)
    iidcVer = Column(Integer)
    interfaceType = Column(Integer)
    isColorCamera = Column(Boolean)
    maximumBusSpeed = Column(Integer)
    modelName = Column(String)
    nodeNumber = Column(Integer)
    pcieBusSpeed = Column(Integer)
    sensorInfo = Column(String)
    sensorResolution = Column(String)
    userDefinedName = Column(String)
    vendorName = Column(String)
    xmlURL1 = Column(String)
    xmlURL2 = Column(String)
    
class FC2Version(Base):
    __tablename__ = 'FC2Version'
    __table_args__ = (
            UniqueConstraint('creationTimeStamp', 'serialNumber'),
            Index('FC2VersionIndex', 'creationTimeStamp', 'serialNumber', unique=True)
            )
    
    id = Column(Integer, primary_key=True)
    creationTimeStamp = Column(DateTime)
    serialNumber = Column(Integer)
    
    major = Column(Integer)
    minor = Column(Integer)
    type = Column(Integer)
    build = Column(Integer)
    
class SystemInfo(Base):
    __tablename__ = 'SystemInfo'
    __table_args__ = (
            UniqueConstraint('creationTimeStamp', 'serialNumber'),
            Index('SystemInfoIndex', 'creationTimeStamp', 'serialNumber', unique=True)
            )
    
    id = Column(Integer, primary_key=True)
    creationTimeStamp = Column(DateTime)
    serialNumber = Column(Integer)
    
    osType = Column(Integer)
    osDescription = Column(String)
    byteOrder = Column(Integer)
    sysMemSize = Column(Integer)
    cpuDescription = Column(String)
    numCpuCores = Column(Integer)
    driverList = Column(String)
    libraryList = Column(String)
    gpuDescription = Column(String)
    screenWidth = Column(Integer)
    screenHeight = Column(Integer)

class ImageMetadata(Base):
    __tablename__ = 'ImageMetadata'
    __table_args__ = (
            UniqueConstraint('creationTimeStamp', 'serialNumber'),
            Index('ImageMetadataIndex', 'creationTimeStamp', 'serialNumber', unique=True)
            )
    
    id = Column(Integer, primary_key=True)
    creationTimeStamp = Column(DateTime)
    serialNumber = Column(Integer)
    
    frameNumber = Column(Integer)
    embeddedTimeStamp = Column(Integer)
    embeddedGain = Column(Integer)
    embeddedShutter = Column(Integer)
    embeddedBrightness = Column(Integer)
    embeddedExposure = Column(Integer)
    embeddedWhiteBalance = Column(Integer)
    embeddedFrameCounter = Column(Integer)
    embeddedStrobePattern = Column(Integer)
    embeddedGPIOPinState = Column(Integer)
    embeddedROIPosition = Column(Integer)
    relativePath = Column(String)
    
    CameraInfo_id = Column(Integer, ForeignKey(CameraInfo.id))
    FC2Version_id = Column(Integer, ForeignKey(FC2Version.id))
    SystemInfo_id = Column(Integer, ForeignKey(SystemInfo.id))
    
    CameraInfo = relationship(CameraInfo, backref=backref('ImageMetadatas'))
    FC2Version = relationship(FC2Version, backref=backref('ImageMetadatas'))
    SystemInfo = relationship(SystemInfo, backref=backref('ImageMetadatas'))
    
def refresh():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    
if __name__ == '__main__':
    refresh()
