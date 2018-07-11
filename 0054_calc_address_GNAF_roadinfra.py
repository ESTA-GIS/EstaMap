'''
Calc ADDRESS GNAF closest ROAD_INFRASTRUCTURE UFI.

Usage:
  calc_address_roadinfra.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  --log_file <file>       Log File name. [default: calc_address_roadinfra.log]
  --log_path <folder>     Folder to store the log file. [default: c:\\temp]
'''
import os
import sys
import logging

from docopt import docopt
import arcpy
import rtree

import log
import dev as gis
import dbpy


def calc_address_gnaf_roadinfra(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)

    logging.info('dropping tables:')
    if dbpy.check_exists('ADDRESS_GNAF_ROADINFRA', conn):
        logging.info('ADDRESS_GNAF_ROADINFRA')
        conn.execute('drop table ADDRESS_GNAF_ROADINFRA')
    

    logging.info('creating ADDRESS_GNAF_ROADINFRA')
    conn.execute('''
    CREATE TABLE [dbo].[ADDRESS_GNAF_ROADINFRA](
        [PFI] [nvarchar](15) NOT NULL,
        [UFI] [int] NULL
    ) ON [PRIMARY]
    ''')
    conn.commit()

##    logging.info('reading ADDRESS_GNAF FIDs')
##    address_fids = {}
##    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ADDRESS_GNAF'),
##                               field_names=['PFI', 'OBJECTID']) as sc:
##        for pfi, oid in sc:
##            address_fids[oid] = pfi
##
##    logging.info('reading ROAD_INFRASTRUCTURE FIDs')
##    roadinfra_fids = {}
##    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD_INFRASTRUCTURE'),
##                               field_names=['UFI', 'OBJECTID']) as sc:
##        for ufi, oid in sc:
##            roadinfra_fids[oid] = ufi
##
##    logging.info('Generate Near Table analysis...')
##    arcpy.GenerateNearTable_analysis(in_features=os.path.join(em.sde, 'ADDRESS'),
##                                     near_features=os.path.join(em.sde, 'ROAD_INFRASTRUCTURE'),
##                                     out_table='in_memory\\near_table')
##
##    logging.info('inserting')
##    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'ADDRESS_ROADINFRA') as sbc, \
##         arcpy.da.SearchCursor(in_table='in_memory\\near_table',
##                               field_names=['IN_FID', 'NEAR_FID']) as sc:
##        for enum, (address_fid, roadinfra_fid) in enumerate(sc):
##            sbc.add_row((address_fids[address_fid], roadinfra_fids[roadinfra_fid]))
##            if enum % 100000 == 0:
##                logging.info(enum)
##                sbc.flush()
##        logging.info(enum)
        

    logging.info('creating rtree ROAD_INFRASTRUCTURE')
    def bulk_load_roadinfra():
        with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD_INFRASTRUCTURE'),
                                   field_names=['UFI', 'SHAPE@X', 'SHAPE@Y']) as sc:
            for enum, (ufi, x, y) in enumerate(sc):
                if enum % 100000 == 0:
                    logging.info(enum)
                yield (int(ufi), (x, y, x, y), None)
            logging.info(enum)
            
    ri_rtree = rtree.Rtree(bulk_load_roadinfra())

    logging.info('looping ADDRESS')
    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'ADDRESS_GNAF_ROADINFRA') as sbc, \
         arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ADDRESS_GNAF'),
                               field_names=['ADDRESS_DETAIL_PID', 'SHAPE@X', 'SHAPE@Y']) as sc:
        for enum, (pfi, x, y) in enumerate(sc):
            ufi = list(ri_rtree.nearest((x, y, x, y)))[0]

            sbc.add_row((pfi, ufi))
            
            if enum % 100000 == 0:
                logging.info(enum)
                sbc.flush()
        logging.info(enum)


if __name__ == '__main__':

    sys.argv.append('--estamap_version=DEV')
    
    with log.LogConsole():
        
        logging.info('parsing args')
        args = docopt(__doc__)

        logging.info('variables')
        estamap_version = args['--estamap_version']
        log_file = args['--log_file']
        log_path = args['--log_path']

        with log.LogFile(log_file, log_path):
            logging.info('start')
            try:
                ###########

                calc_address_gnaf_roadinfra(estamap_version)
                
            

                ###########   
            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')

