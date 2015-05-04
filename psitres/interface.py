from psitres import utils
from psitres import models
from psitres import pe
from lxml import etree
from datetime import datetime
import multiprocessing as mp
import click
import bisect
import os
import time
import re
import warnings
import csv
from sqlalchemy.inspection import inspect


def _binary_search(a, x):
    'Locate the leftmost value exactly equal to x'
    i = bisect.bisect_left(a, x)
    if i != len(a) and a[i] == x:
        return i
    raise ValueError

    
def _parse_fname(fname):
    re_creationTimeStamp = r'(?P<creationTimeStamp>\d{8}T\d{6}\.\d{6})'
    re_serialNumber = r'(?P<serialNumber>\d+)'
    re_modelName = r'(?P<modelName>[a-zA-Z]\w*)'
    re_frameNumber = r'(?P<frameNumber>\d+)'
    re_ext = r'\w{3}'
    patterns = [r'^{creationTimeStamp}\.{serialNumber}\.{frameNumber}\.{ext}$'.format(creationTimeStamp=re_creationTimeStamp,
                                                                                      serialNumber=re_serialNumber,
                                                                                      modelName=re_modelName,
                                                                                      frameNumber=re_frameNumber,
                                                                                      ext=re_ext),
                r'^{creationTimeStamp}_{serialNumber}_{frameNumber}\.{ext}$'.format(creationTimeStamp=re_creationTimeStamp,
                                                                                    serialNumber=re_serialNumber,
                                                                                    modelName=re_modelName,
                                                                                    frameNumber=re_frameNumber,
                                                                                    ext=re_ext),
                r'^{creationTimeStamp}_{serialNumber}_{frameNumber}\.{ext}_{modelName}\.{ext}$'.format(creationTimeStamp=re_creationTimeStamp,
                                                                                                        serialNumber=re_serialNumber,
                                                                                                        modelName=re_modelName,
                                                                                                        frameNumber=re_frameNumber,
                                                                                                        ext=re_ext),
                r'^{creationTimeStamp}\.{serialNumber}\.{modelName}\.{ext}$'.format(creationTimeStamp=re_creationTimeStamp,
                                                                                     serialNumber=re_serialNumber,
                                                                                     modelName=re_modelName,
                                                                                     ext=re_ext),
                r'^{creationTimeStamp}_{serialNumber}_{modelName}\.{ext}$'.format(creationTimeStamp=re_creationTimeStamp,
                                                                                   serialNumber=re_serialNumber,
                                                                                   modelName=re_modelName,
                                                                                   ext=re_ext), ]    
    for pattern in patterns:
        try:
            return re.match(pattern, fname).groupdict()
        except AttributeError:
            continue
        
        
def _cached_frameNumbers(root_path, rel_dir):
    glob_key = _cached_frameNumbers, root_path, rel_dir
    try:
        return globals()[glob_key]
    except KeyError:
        frm_map = {}
        for f in os.listdir(os.path.join(root_path, rel_dir)):
            a = _parse_fname(f)
            try:
                val = int(a['frameNumber'])
                key = int(a['serialNumber']), datetime.strptime(a['creationTimeStamp'], '%Y%m%dT%H%M%S.%f')
                frm_map[key] = val
            except KeyError:
                continue
        keys = sorted(frm_map)
        vals = [frm_map[k] for k in keys]
        
        globals()[glob_key] = (keys, vals)
        return globals()[glob_key]
            
    
def _xml2model(root_path, rel_path, session):
    rel_dir, file_ = os.path.split(rel_path)
    attributes = _parse_fname(file_)    
    modelName = attributes.pop('modelName')
    
    if modelName == 'ImageMetadata' and 'frameNumber' not in attributes:        
        keys, vals = _cached_frameNumbers(root_path, rel_dir)        
        try:
            key = int(attributes['serialNumber']), datetime.strptime(attributes['creationTimeStamp'], '%Y%m%dT%H%M%S.%f')
            i = _binary_search(keys, key)
            attributes['frameNumber'] = vals[i]
        except ValueError:
            warnings.warn('unable to find frameNumber for ImageMetadata: {}'.format(attributes), RuntimeWarning, 2)
        
#         re_frameNumber = r'(?P<frameNumber>\d+)'
#         re_ext = r'\w{3}'
#         pattern = r'^{creationTimeStamp}\.{serialNumber}\.{frameNumber}\.{ext}$'.format(creationTimeStamp=re.escape(attributes['creationTimeStamp']),
#                                                                                         serialNumber=re.escape(attributes['serialNumber']),
#                                                                                         frameNumber=re_frameNumber,
#                                                                                         ext=re_ext)
#         
#         for f in os.listdir(os.path.join(root_path, rel_dir)):
#             try:
#                 attributes['frameNumber'] = re.match(pattern, f).group('frameNumber')
#                 break
#             except AttributeError: 
#                 continue
#         else:
#             warnings.warn('unable to find frameNumber for ImageMetadata: {}'.format(attributes), RuntimeWarning, 2)
                
    if 'frameNumber' in attributes:
        attributes['frameNumber'] = int(attributes['frameNumber'])
    attributes['creationTimeStamp'] = datetime.strptime(attributes['creationTimeStamp'], '%Y%m%dT%H%M%S.%f')
    attributes['serialNumber'] = int(attributes['serialNumber'])
            
    mdl = getattr(models, modelName)
    
    ind = [ind for ind in models.inspector.get_indexes(mdl.__tablename__) 
           if ind['name'] == 'creationTimeStamp_serialNumber']
    if len(ind) != 1:
        raise ValueError('index creationTimeStamp_serialNumber does not exist in table {}'.format(mdl.__tablename__))
    ind = ind[0]['column_names']
    inst = utils.read_or_instantiate(session, mdl, *ind, **attributes)
    
    if not inspect(inst).persistent:
        tbl = mdl.__table__    
        try:
            with open(os.path.join(root_path, rel_path), 'r') as fp:
                xroot = etree.fromstring(fp.read())
            xroot = xroot.find(modelName)
            
            for attr in xroot:
                col = getattr(tbl.columns, attr.tag)
#                 attributes[attr.tag] = col.type.python_type(attr.text)
                setattr(inst, attr.tag, col.type.python_type(attr.text))
        except etree.XMLSyntaxError:
            warnings.warn('unable to parse metadata for {}: {}'.format(modelName, attributes), RuntimeWarning, 2)
    
    return inst


def _commit_init_files(output_dir, init_fnames):
    pe._mp_print('_commit_init_files', len(init_fnames))
    session = models.Session()
    instcs = [_xml2model(output_dir, f, session) for f in init_fnames]
    ind = {(inst.serialNumber, inst.creationTimeStamp) for inst in instcs}
    session.add_all(instcs)    
    session.commit()
    return ind


def _commit_data_files(output_dir, data_files, init_file_times):
    pe._mp_print('_commit_data_files', len(data_files))
    session = models.Session()
    instcs = [_xml2model(output_dir, f, session) for f in data_files]
    for inst in instcs:
        if not inspect(inst).persistent:
            ts = init_file_times[inst.serialNumber]
            ts = ts[:bisect.bisect_left(ts, inst.creationTimeStamp)]
            ts = utils.take_closest(ts, inst.creationTimeStamp)        
            attrs = {'creationTimeStamp':ts, 'serialNumber':inst.serialNumber}
            
            for fk in models.inspector.get_foreign_keys(inst.__tablename__):
                if len(fk['referred_columns']) != 1 or len(fk['constrained_columns']) != 1:
                    raise ValueError('composite foreign keys are not supported')
                fk_mdl = getattr(models, fk['referred_table'])
                fk_inst = session.query(fk_mdl).filter_by(**attrs).one()
                fk_id = getattr(fk_inst, fk['referred_columns'][0])
                setattr(inst, fk['constrained_columns'][0], fk_id)
    session.add_all(instcs)
    session.commit()
    

@click.command()
@click.option('--data_dir', help='root directory where images and metadata were captured', required=True)
@click.option('--recreate', help='drops all data and the schema in the database and recreates schema', is_flag=True)    
def populate_db(data_dir, recreate):
    if recreate and click.confirm('Are you sure you want to delete all data in the database?', abort=True):
        models.recreate()
    
    init_fnames = [f for f in sorted(os.listdir(data_dir)) 
                   if os.path.splitext(f)[1] == '.xml']
    t = time.time()
    ind = pe.ParFor()(pe.delayed(_commit_init_files)(data_dir, fnames) 
                      for fnames in utils.partition(init_fnames, mp.cpu_count()))
    t = time.time() - t
    print '{} items / {} seconds = {} items per second'.format(len(init_fnames), t, len(init_fnames) / t)    
    
    init_file_times = {}
    ind = sorted({v for i in ind for v in i})    
    for serialNumber, creationTimeStamp in ind:
        try:
            init_file_times[serialNumber].append(creationTimeStamp)
        except KeyError:
            init_file_times[serialNumber] = [creationTimeStamp]
    
    data_dirnames = [os.path.join(d1, d2) 
                     for d1 in sorted(os.listdir(data_dir)) 
                     if os.path.isdir(os.path.join(data_dir, d1))
                     for d2 in sorted(os.listdir(os.path.join(data_dir, d1))) 
                     if os.path.isdir(os.path.join(data_dir, d1, d2))]
    
    for d in data_dirnames:
        print datetime.strptime(d, '%Y%m%d\\%H')
        data_files = [os.path.join(d, f)
                      for f in sorted(os.listdir(os.path.join(data_dir, d)))
                      if os.path.splitext(f)[1] == '.xml']
        
        t = time.time()
        ind = pe.ParFor()(pe.delayed(_commit_data_files)(data_dir, fnames, init_file_times) 
                          for fnames in utils.partition(data_files, mp.cpu_count()))
        t = time.time() - t
        print '{} items / {} seconds = {} items per second'.format(len(data_files), t, len(data_files) / t)
    

@click.command()
@click.option('--start', help='starting datetime formatted as %Y-%m-%d %H:%M:%S.%f', required=True)    
@click.option('--stop', help='stopping datetime formatted as %Y-%m-%d %H:%M:%S.%f', required=True)    
@click.option('--seperator', help='seperator used in file names: either "." or "_"', required=True)    
@click.option('--serial_numbers', help='serial numbers for each camera in stereo pair', nargs=2, required=True)    
@click.option('--data_dir', help='root directory where images and metadata were captured', required=True)    
@click.option('--out_file', help='output file name where paths to stereo pairs are written', required=True)    
def find_pairs(start, stop, seperator, serial_numbers, data_dir, out_file):
    session = models.Session()    
    start = datetime.strptime(start, '%Y-%m-%d %H:%M:%S.%f') 
    stop = datetime.strptime(stop, '%Y-%m-%d %H:%M:%S.%f') 
    serial_numbers = [int(s) for s in serial_numbers]
    if len(serial_numbers) != 2:
        raise ValueError('too many serial_numbers in config file')

    qry = session.query(models.ImageMetadata.creationTimeStamp, models.ImageMetadata.serialNumber, models.ImageMetadata.frameNumber)
    qry = qry.filter(start <= models.ImageMetadata.creationTimeStamp,
                     models.ImageMetadata.creationTimeStamp < stop,
                     models.ImageMetadata.serialNumber.in_(serial_numbers))
    qry = qry.order_by(models.ImageMetadata.creationTimeStamp)
    
    serial2times = {}
    serialtime2frame = {}
    for inst in qry: 
        try:
            serial2times[inst.serialNumber].append(inst.creationTimeStamp)
        except KeyError:
            serial2times[inst.serialNumber] = [inst.creationTimeStamp]
        finally:
            serialtime2frame[inst.serialNumber, inst.creationTimeStamp] = inst.frameNumber
        
    pairs = []
    for ts0 in serial2times[serial_numbers[0]]:        
        ts1 = utils.take_closest(serial2times[serial_numbers[1]], ts0)
        ts2 = utils.take_closest(serial2times[serial_numbers[0]], ts1)        
        if ts0 == ts2:
            im0 = seperator.join(map(str,
                                     (ts0.strftime('%Y%m%dT%H%M%S.%f'),
                                      serial_numbers[0],
                                      serialtime2frame[serial_numbers[0], ts0]))) + '.jpg'
            im0 = os.path.join(data_dir, ts0.strftime('%Y%m%d'), ts0.strftime('%H'), im0)
            im1 = seperator.join(map(str,
                                     (ts1.strftime('%Y%m%dT%H%M%S.%f'),
                                      serial_numbers[1],
                                      serialtime2frame[serial_numbers[1], ts1]))) + '.jpg'
            im1 = os.path.join(data_dir, ts1.strftime('%Y%m%d'), ts1.strftime('%H'), im1)
            if not (os.path.isfile(im0) and os.path.isfile(im1)):
                raise RuntimeError('one or more computed image paths do not exist: {} {}'.format(im0, im1))
            pairs.append([im0, im1])
        else:
            warnings.warn('[{},{}]: backtracking mismatch of {:.2f}s'.format(serial_numbers[0],
                                                                             ts0.isoformat(),
                                                                             abs((ts1 - ts0).total_seconds())), RuntimeWarning, 2)
    
    with open(out_file, 'w+') as fp:
        writer = csv.writer(fp, lineterminator='\n')
        writer.writerows(pairs)


@click.group()
def cli():
    pass
        

cli.add_command(find_pairs)
cli.add_command(populate_db)
                
