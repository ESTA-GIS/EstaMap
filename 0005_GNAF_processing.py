'''
Process and filter GNAF dataset to filter for address points on the border of NSW and SA
using Victoria polygon (which extends 50 Km to NSW and 15 Km to SA)

  Sets:
	the DB size = 15gb.
	recovery model = simple

  Grants read access to:
	ESTAMIS
	ArcGISSOC


Usage:
  load_gnaf.py [options]

Options:
  --gnaf_version <version>  GNAF Version
  --estamap_version <version>  ESTAMAP Version
  --auth_file <file>        ArcServer Authorization File
  --log_file <file>         Log File name. [default: 005gnaf_processing.log]
  --log_path <folder>       Folder to store the log file. [default: C:\\Temp\\EM19]
'''

import dbpy
import shapely.wkt
import shapely.ops
import shapely.prepared
import pyproj
import datetime
import rtree
import dbpy
import csv
import pandas as pd
import sys
import os
from docopt import docopt 
import logging
import log
import dev as gis
import glob

def exec_gnaf_sqlscripts(sql_script,database = None):
    if database is None: database = gnaf.database_name
    logging.debug('create gnaf tables')
    sql = os.path.join(r'M:\estamap\DEV', 'sql','GNAF', sql_script)
    logging.debug('running sql script: {}'.format(sql))

    dbpy.exec_script(gnaf.server, database, sql)
    logging.info('Executed sql script: {}'.format(sql))

def gnaf_bulk_import():

    gnaf_files = {
                    'NSW_ADDRESS_DEFAULT_GEOCODE_psv.psv':'ADDRESS_DEFAULT_GEOCODE' ,
                    'NSW_ADDRESS_DETAIL_psv.psv':'ADDRESS_DETAIL' ,
                    'NSW_ADDRESS_SITE_GEOCODE_psv.psv': 'ADDRESS_SITE_GEOCODE',
                    'NSW_LOCALITY_POINT_psv.psv': 'LOCALITY_POINT',
                    'NSW_LOCALITY_psv.psv':'LOCALITY' ,
                    'NSW_STATE_psv.psv':'STATE' ,
                    'NSW_STREET_LOCALITY_POINT_psv.psv':'STREET_LOCALITY_POINT' ,
                    'NSW_STREET_LOCALITY_psv.psv':'STREET_LOCALITY' ,
                    'SA_ADDRESS_DEFAULT_GEOCODE_psv.psv':'ADDRESS_DEFAULT_GEOCODE' ,
                    'SA_ADDRESS_DETAIL_psv.psv':'ADDRESS_DETAIL' ,
                    'SA_ADDRESS_SITE_GEOCODE_psv.psv': 'ADDRESS_SITE_GEOCODE',
                    'SA_LOCALITY_POINT_psv.psv': 'LOCALITY_POINT',
                    'SA_LOCALITY_psv.psv':'LOCALITY' ,
                    'SA_STATE_psv.psv':'STATE' ,
                    'SA_STREET_LOCALITY_POINT_psv.psv':'STREET_LOCALITY_POINT' ,
                    'SA_STREET_LOCALITY_psv.psv':'STREET_LOCALITY'
                    }

    import_files = [os.path.basename(f) for f in glob.glob(os.path.join(gnaf.path,'*.psv'))]
    #print set(gnaf_files.keys())-set(import_files)
    if(set(gnaf_files.keys())-set(import_files)):
        logging.warning(('GDB files not found/extracted',set(gnaf_files.keys())-set(import_files)))
        # logging.warning('******************************* {}'.format(import_files))
        sys.exit()


    for filename, table in gnaf_files. items():
        importFile = os.path.join(r'M:\estamap\DEV', 'Export', 'GNAF' , filename)
        with dbpy.SQL_BULK_COPY(gnaf.server, gnaf.database_name, table, True) as sbc_gnaf, \
             open(importFile, 'rb') as f:
            
            reader = csv.reader(f, delimiter='|')
            header = reader.next()
            for e, row in enumerate(reader):
                sbc_gnaf.add_row(row)
                if e % 50000 == 0:
                    sbc_gnaf.flush()
            print '{0} Load completed'.format(filename)
                     
def data_filtering(table):
    
    lst_polygons = []
    conn_estamap = dbpy.create_conn_pyodbc(gnaf.server,em.database_name)
    conn_gnaf = dbpy.create_conn_pyodbc(gnaf.server,gnaf.database_name)

    cursor_estamap = conn_estamap.cursor()
    cursor_gnaf = conn_gnaf.cursor()

    for wkt, in cursor_estamap.execute('SELECT SHAPE FROM VICTORIA_POLYGON'):
        geom = shapely.wkt.loads(wkt)
        lst_polygons.append(geom)

    vic_polygon = shapely.ops.cascaded_union(lst_polygons)
    vic_polygon_prepared = shapely.prepared.prep(vic_polygon)
    vicgrid_proj = pyproj.Proj(init='EPSG:3111')
    count = 0
    count_false = 0

    cursor_gnaf.execute('truncate table {0}_filteredPID'.format(table))
    cursor_gnaf.commit()
   
    start_time = datetime.datetime.now()
    print start_time, ' - START Data Filtering for ', table

    with dbpy.SQL_BULK_COPY(gnaf.server, gnaf.database_name, table+'_FILTEREDPID') as sbc:
        for row in cursor_gnaf.execute('SELECT {0}_PID,LONGITUDE, LATITUDE FROM {0}'.format(table)):
            
        ##for enum, row in enumerate(cursor_gnaf.execute('select address_detail_pid, latitude, longitude from address_default_geocode')):
            pid, lon, lat = row
            lon_p, lat_p = vicgrid_proj(lon, lat)
            point = shapely.geometry.Point(lon_p, lat_p)
            if vic_polygon_prepared.intersects(point):
                count += 1
                sbc.add_row([pid])
                if count % 10000 == 0:
                    sbc.flush()
            else:
                count_false += 1
        ##    if enum % 100000 == 0:
        ##        print time.ctime(), enum
        ##        print 'inside polygon =',count,',      Ouside Polygon =', count_false
        end_time = datetime.datetime.now()
        elapsed = end_time - start_time
        
        ##if count >=0:
        ##    print 'Total points inside Victoria Polygon = {0}  : Percent Data = {1}%'.format((count),(count*100 / (count + count_false)))
        ##    print 'Total points outside VIC polygon = {0} :   : Percent Data = {1}%'.format((count_false),(count_false*100 / (count + count_false)))
        print 'Time taken for filtering {0} : {1} \n'.format(table,elapsed)
        return elapsed


if __name__ == '__main__':

    sys.argv.append('--gnaf_version=201805')
    sys.argv.append('--estamap_version=19') ##ENSURE ESTAMAP DATABASE HAS GOT VICTORIA POLYGON TABLE
    with log.LogConsole(level='WARNING'):
        
        logging.debug('parsing args')
        args = docopt(__doc__)

        logging.debug('variables')
        gnaf_version = args['--gnaf_version']
        estamap_version = args['--estamap_version']
        log_file = args['--log_file']
        log_path = args['--log_path']
        gnaf = gis.GNAF(gnaf_version)
        em = gis.ESTAMAP(estamap_version)

        conn_estamap = dbpy.create_conn_pyodbc(em.server,em.database_name)
        cursor_estamap = conn_estamap.cursor()
        
        with log.LogFile(log_file, log_path):
            logging.debug('start')
            try:
                # cursor_estamap.execute(r"""IF OBJECT_ID('dbo.ADDRESS_GNAF', 'U') IS NOT NULL DROP TABLE dbo.ADDRESS_GNAF""" )
                # cursor_estamap.commit()
                # drop and recreate gnaf tables                
                exec_gnaf_sqlscripts('01Create_GNAF_tables.sql')
                #bulk load of gnaf datasets into gnaf database
                gnaf_bulk_import()
                print 'Bulk Load of GNAF tables completed'
                # Filter for duplicate rows on the loaded GNAF tables             
                exec_gnaf_sqlscripts('03RemoveDuplicates_GNAF_tables.sql')
                #adding primary key to tables in gnaf databases
                exec_gnaf_sqlscripts('04Primarykeys_GNAF_tables.sql')
                #Adding Geometry column to tables
                exec_gnaf_sqlscripts('05Geomety_GNAF_tables.sql')

                #FILTERING DATA TO PRODUCE DATASET FOR VICTORIA BOUNDARY
                lst_sourceTables = ['ADDRESS_DEFAULT_GEOCODE','ADDRESS_SITE_GEOCODE','LOCALITY_POINT','STREET_LOCALITY_POINT']
                #lst_sourceTables = ['STREET_LOCALITY_POINT']
                tot_time = datetime.datetime(2018,1,1)
                for feature in lst_sourceTables:
                    tot_time += data_filtering(feature)
                print "Total Time taken for filtering all tables : ",tot_time.time()

                #Creating View to pull necessary address details
                exec_gnaf_sqlscripts('07Create_GNAF_View.sql')
                #Pysicalising the GNAF View - Create a table on ESTAMAP database based on the view
                cursor_estamap.execute(r"""IF OBJECT_ID('dbo.ADDRESS_GNAF', 'U') IS NOT NULL DROP TABLE dbo.ADDRESS_GNAF""" )
                cursor_estamap.commit()
                cursor_estamap.execute('SELECT * INTO ADDRESS_GNAF FROM '+gnaf.database_name+'.DBO.GNAF_VIEW')
                cursor_estamap.commit()
                print 'Physicalised GNAF view to '+ gnaf.database_name
                #Creating spatial index on the ESTAMAP GNAF table
                exec_gnaf_sqlscripts('09SpatialIndexing_ESTAMAP_GNAF_table.sql',em.database_name)
                
                
            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.debug('finished')

            cursor_estamap.close()
            conn_estamap.close()

