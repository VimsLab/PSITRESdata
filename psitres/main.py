import ConfigParser
import os
from datetime import datetime
from lxml import etree
import model
from bisect import bisect_left
from sqlalchemy import inspect
from sqlalchemy.orm.exc import NoResultFound
from collections import OrderedDict
from sqlalchemy.engine.reflection import Inspector

def read_or_instantiate(session, model_, **kwargs):
    try:
        insp = Inspector.from_engine(model.engine)
        ind = insp.get_indexes(model_.__tablename__)
        assert len(ind) == 1
        ind = {k:kwargs[k] for k in ind[0]['column_names']}
        # TODO: this will be too slow, try using Index
        return session.query(model_).filter_by(**ind).one()
    except NoResultFound:
        return model_(**kwargs)

def takeClosest(myList, myNumber):
    """
    Assumes myList is sorted. Returns closest value to myNumber.

    If two numbers are equally close, return the smallest number.
    
    This is generally faster than: 
        return min(myList, key=lambda x:abs(x - myNumber))
    """
    pos = bisect_left(myList, myNumber)
    if pos == 0:
        return myList[0]
    if pos == len(myList):
        return myList[-1]
    before = myList[pos - 1]
    after = myList[pos]
    if after - myNumber < myNumber - before:
        return after    
    else:
        return before

def xml2model(session, rootabspath, abspath):
    attrs = {}
    head, tail = os.path.split(abspath)
    root, _ = os.path.splitext(tail)
    fparts = root.split('_')
    if len(fparts) == 4:
        creationTimeStamp, serialNumber, frameNumber, structName = fparts
        relativePath = os.path.join(os.path.relpath(head, rootabspath), '_'.join((creationTimeStamp, serialNumber, frameNumber)))
        frameNumber = int(frameNumber.split('.')[0])
        attrs['relativePath'] = relativePath
        attrs['frameNumber'] = frameNumber 
    elif len(fparts) == 3:
        creationTimeStamp, serialNumber, structName = fparts
    else:
        raise TypeError('abspath could not be parsed')
    
    mdl = getattr(model, structName)
    tbl = mdl.__table__
    
    creationTimeStamp = datetime.strptime(creationTimeStamp, '%Y%m%dT%H%M%S.%f')
    serialNumber = int(serialNumber)
    attrs['creationTimeStamp'] = creationTimeStamp
    attrs['serialNumber'] = serialNumber
    
    with open(abspath, 'r') as fp:
        xroot = etree.fromstring(fp.read())
    xroot = xroot.find(structName)
    
    for attr in xroot:
        col = getattr(tbl.columns, attr.tag)
        attrs[attr.tag] = col.type.python_type(attr.text)
    
    return read_or_instantiate(session, mdl, **attrs)

def populate_db():    
    session = model.Session()    
    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    output_dir = config.get('psitres', 'output_directory')
    
    init_fnames = [f 
                   for f in sorted(os.listdir(output_dir)) 
                   if os.path.splitext(f)[1] == '.xml']
    
    data_dirnames = [os.path.join(d1, d2) 
                     for d1 in sorted(os.listdir(output_dir)) 
                     if os.path.isdir(os.path.join(output_dir, d1))
                     for d2 in sorted(os.listdir(os.path.join(output_dir, d1))) 
                     if os.path.isdir(os.path.join(output_dir, d1, d2))]
    
    rel_insts = {}
    for f in sorted(init_fnames):
        print f
        inst = xml2model(session, output_dir, os.path.join(output_dir, f))
        r = rel_insts.get(inst.creationTimeStamp, [])
        r.append(inst)
        rel_insts[inst.creationTimeStamp] = r
    rel_insts = OrderedDict((ts, rel_insts[ts]) for ts in sorted(rel_insts))
    
    for d in sorted(data_dirnames):
        for f in sorted(os.listdir(os.path.join(output_dir, d))):
            if os.path.splitext(f)[1] == '.xml':
                print f
                inst = xml2model(session, output_dir, os.path.join(output_dir, d, f))
                if not inspect(inst).persistent:
                    rel_ts = rel_insts.keys()
                    rel_ts = rel_ts[:bisect_left(rel_ts, inst.creationTimeStamp)]
                    rel_ts = takeClosest(rel_ts, inst.creationTimeStamp)
                    for rel_inst in rel_insts[rel_ts]:
                        rel_inst.ImageMetadatas.append(inst) 
    
    for instances in rel_insts.values():
        for inst in instances:
            session.add(inst)
                       
    session.commit()
    
def find_pairs():
    session = model.Session()    
    t0 = datetime(2015, 3, 27, 8, 40)
    t1 = datetime(2015, 3, 27, 9, 40)
    serialNumbers = [13020556, 13232653]
    
    cams = {}
    for inst in session.query(model.ImageMetadata).filter(t0 < model.ImageMetadata.creationTimeStamp,
                                                       model.ImageMetadata.creationTimeStamp < t1,
                                                       model.ImageMetadata.serialNumber.in_(serialNumbers)):
        metadata = cams.get(inst.serialNumber, [])
        metadata.append(inst)
        cams[inst.serialNumber] = metadata
        
    for serialNumber in cams:
        cams[serialNumber] = {meta.creationTimeStamp:meta for meta in cams[serialNumber]}
        
    pairs = []
    timestamps = [sorted(cams[serialNumber]) for serialNumber in serialNumbers]    
    for ts0 in timestamps[0]:        
        ts1 = takeClosest(timestamps[1], ts0)
        ts2 = takeClosest(timestamps[0], ts1)
        
        if ts0 == ts2:
            pairs.append((ts0.isoformat(), cams[serialNumbers[0]][ts0].relativePath, ts1.isoformat(), cams[serialNumbers[1]][ts1].relativePath))
            print pairs[-1]
    
if __name__ == '__main__':
    find_pairs()

