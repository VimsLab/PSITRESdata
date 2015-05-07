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
import joblib


def _parse_fname(fname):
    re_creationTimeStamp = r'(?P<creationTimeStamp>\d{8}T\d{6}(\.\d{6})?)'
    re_serialNumber = r'(?P<serialNumber>\d+)'
    re_modelName = r'(?P<modelName>[a-zA-Z]\w*)'
    re_frameNumber = r'(?P<frameNumber>\d+)'
    re_ext = r'\w{3}'
    pattern = r'^{creationTimeStamp}(\.|_){serialNumber}((\.|_){frameNumber}\.{ext})?((\.|_){modelName}\.{ext})?$'.format(
                                                                  creationTimeStamp=re_creationTimeStamp,
                                                                  serialNumber=re_serialNumber,
                                                                  modelName=re_modelName,
                                                                  frameNumber=re_frameNumber,
                                                                  ext=re_ext)
    return re.match(pattern, fname).groupdict()
        
        
def _cached_frameNumbers(root_path, rel_dir):
    cache_key = _cached_frameNumbers, root_path, rel_dir
    try:
        return globals()[cache_key]
    except KeyError:
        frameNumber_index = {}
        for f in os.listdir(os.path.join(root_path, rel_dir)):
            attributes = _parse_fname(f)
                
            if attributes['frameNumber'] is not None:
                frameNumber = int(attributes['frameNumber'])
                key = (int(attributes['serialNumber']),)
                try:
                    key = key + (datetime.strptime(attributes['creationTimeStamp'], '%Y%m%dT%H%M%S.%f'),)
                except ValueError:
                    key = key + (datetime.strptime(attributes['creationTimeStamp'], '%Y%m%dT%H%M%S'),)                    
                frameNumber_index[key] = frameNumber
            else:
                continue
        index = sorted(frameNumber_index)
        frameNumbers = [frameNumber_index[k] for k in index]
        
        globals()[cache_key] = (index, frameNumbers)
        return globals()[cache_key]
            
    
def _xml2model(root_path, rel_path, session):
    rel_dir, fname = os.path.split(rel_path)
    attributes = _parse_fname(fname)    
    modelName = attributes.pop('modelName')
    
    if (modelName == 'ImageMetadata') and (attributes['frameNumber'] is None):        
        index, frameNumbers = _cached_frameNumbers(root_path, rel_dir)        
        try:
            key = (int(attributes['serialNumber']),)
            try:
                key = key + (datetime.strptime(attributes['creationTimeStamp'], '%Y%m%dT%H%M%S.%f'),)
            except ValueError:
                key = key + (datetime.strptime(attributes['creationTimeStamp'], '%Y%m%dT%H%M%S'),)                
            i = utils.binary_search(index, key)
            attributes['frameNumber'] = frameNumbers[i]
        except ValueError:
            del attributes['frameNumber']
            warnings.warn('unable to find frameNumber for ImageMetadata: {}'.format(attributes), RuntimeWarning, 2)
        
    if attributes['frameNumber'] is not None:
        attributes['frameNumber'] = int(attributes['frameNumber'])
    else:
        del attributes['frameNumber']
    
    try:
        attributes['creationTimeStamp'] = datetime.strptime(attributes['creationTimeStamp'], '%Y%m%dT%H%M%S.%f')
    except ValueError:
        attributes['creationTimeStamp'] = datetime.strptime(attributes['creationTimeStamp'], '%Y%m%dT%H%M%S')

    attributes['serialNumber'] = int(attributes['serialNumber'])
            
    model = getattr(models, modelName)    
    index = [index for index in models.inspector.get_indexes(model.__tablename__) 
           if index['name'] == 'creationTimeStamp_serialNumber']
    if len(index) != 1:
        raise ValueError('index creationTimeStamp_serialNumber does not exist in table {}'.format(model.__tablename__))
    index = index[0]['column_names']
    instance = utils.read_or_instantiate(session, model, *index, **attributes)
    
    if not inspect(instance).persistent:
        table = model.__table__    
        try:
            with open(os.path.join(root_path, rel_path), 'r') as fp:
                root_node = etree.fromstring(fp.read())
            root_node = root_node.find(modelName)
            
            for attribute in root_node:
                column = getattr(table.columns, attribute.tag)
                setattr(instance, attribute.tag, column.type.python_type(attribute.text))
        except etree.XMLSyntaxError:
            warnings.warn('unable to parse metadata for {}: {}'.format(modelName, attributes), RuntimeWarning, 2)
    
    return instance


def _commit_init_files(output_dir, init_fnames):
    pe.mp_print('_commit_init_files', len(init_fnames))
    session = models.Session()
    instances = [_xml2model(output_dir, f, session) for f in init_fnames]
    index = {(inst.serialNumber, inst.creationTimeStamp) for inst in instances}
    session.add_all(inst for inst in instances if not inspect(inst).persistent)    
    session.commit()
    return index


def _commit_data_files(output_dir, data_files, init_file_times):
    pe.mp_print('_commit_data_files', len(data_files))
    session = models.Session()
    instances = (_xml2model(output_dir, f, session) for f in data_files)
    instances = [instance for instance in instances if not inspect(instance).persistent]
    for instance in instances:
        timestamp = init_file_times[instance.serialNumber]
        timestamp = timestamp[:bisect.bisect_left(timestamp, instance.creationTimeStamp)]
        timestamp = utils.take_closest(timestamp, instance.creationTimeStamp)        
        attributes = {'creationTimeStamp':timestamp, 'serialNumber':instance.serialNumber}        
        for fk in models.inspector.get_foreign_keys(instance.__tablename__):
            if len(fk['referred_columns']) != 1 or len(fk['constrained_columns']) != 1:
                raise ValueError('composite foreign keys are not supported')
            fk_model = getattr(models, fk['referred_table'])
            fk_instance = session.query(fk_model).filter_by(**attributes).one()
            fk_id = getattr(fk_instance, fk['referred_columns'][0])
            setattr(instance, fk['constrained_columns'][0], fk_id)
    session.add_all(instances)
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
    index = joblib.Parallel(-1)(joblib.delayed(_commit_init_files)(data_dir, fnames) 
                      for fnames in utils.partition(init_fnames, mp.cpu_count()))
    t = time.time() - t
    print '{} items / {} seconds = {} items per second'.format(len(init_fnames), t, len(init_fnames) / t)    
    
    init_timestamps = {}
    index = sorted({v for i in index for v in i})    
    for serialNumber, creationTimeStamp in index:
        try:
            init_timestamps[serialNumber].append(creationTimeStamp)
        except KeyError:
            init_timestamps[serialNumber] = [creationTimeStamp]
    
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
        index = joblib.Parallel(-1)(joblib.delayed(_commit_data_files)(data_dir, fnames, init_timestamps) 
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
    
    query = session.query(models.ImageMetadata.creationTimeStamp,
                          models.ImageMetadata.serialNumber,
                          models.ImageMetadata.frameNumber)
    query = query.filter(start <= models.ImageMetadata.creationTimeStamp,
                         models.ImageMetadata.creationTimeStamp < stop,
                         models.ImageMetadata.serialNumber.in_(serial_numbers))
    query = query.order_by(models.ImageMetadata.creationTimeStamp)
    print '{} potential pairs selected'.format(query.count() / 2)
    
    timestamp_index = {}
    frameNumber_index = {}
    for instance in query: 
        try:
            timestamp_index[instance.serialNumber].append(instance.creationTimeStamp)
        except KeyError:
            timestamp_index[instance.serialNumber] = [instance.creationTimeStamp]
        finally:
            frameNumber_index[instance.serialNumber, instance.creationTimeStamp] = instance.frameNumber
    print '{} maximum possible pairs'.format(min(len(v) for v in timestamp_index.values()))
    
    pairs = []
    for timestamp in timestamp_index[serial_numbers[0]]:        
        forward_match = utils.take_closest(timestamp_index[serial_numbers[1]], timestamp)
        backward_match = utils.take_closest(timestamp_index[serial_numbers[0]], forward_match)        
        if timestamp == backward_match:
            im0 = seperator.join(map(str,
                                     (timestamp.strftime('%Y%m%dT%H%M%S.%f'),
                                      serial_numbers[0],
                                      frameNumber_index[serial_numbers[0], timestamp]))) + '.jpg'
            im0 = os.path.join(data_dir, timestamp.strftime('%Y%m%d'), timestamp.strftime('%H'), im0)
            im1 = seperator.join(map(str,
                                     (forward_match.strftime('%Y%m%dT%H%M%S.%f'),
                                      serial_numbers[1],
                                      frameNumber_index[serial_numbers[1], forward_match]))) + '.jpg'
            im1 = os.path.join(data_dir, forward_match.strftime('%Y%m%d'), forward_match.strftime('%H'), im1)
            if not (os.path.isfile(im0) and os.path.isfile(im1)):
                warnings.warn('some computed image paths do not exist')
            pairs.append([im0, im1])
        else:
            warnings.warn('some backtracking mismatches')    
    
    with open(out_file, 'w+') as fp:
        writer = csv.writer(fp, lineterminator='\n')
        writer.writerows(pairs)
    print '{} pairs written to disk'.format(len(pairs))
    

@click.group()
def cli():
    pass
        

cli.add_command(find_pairs)
cli.add_command(populate_db)
                
