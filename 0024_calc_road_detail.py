'''

Usage:
  calc_road_geom_detail.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  --log_file <file>       Log File name. [default: calc_road_geom_detail.log]
  --log_path <folder>     Folder to store the log file. [default: c:\\temp]
'''
import os
import sys
import time
import logging

from docopt import docopt
import arcpy
import clr

import log
import dev as gis
import dbpy

def create_road_detail_table(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    sql_script = os.path.join(em.path, 'sql', 'detail_tables', 'create_road_detail.sql')
    logging.info('running sql script: {}'.format(sql_script))

    dbpy.exec_script(em.server, em.database_name, sql_script)


def calc_road_detail(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    cursor = em.conn.cursor()
    
    logging.info('calc geom attr')
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD'),
                               field_names=['PFI', 'SHAPE@'],
                               sql_clause=(None, 'ORDER BY PFI')) as sc, \
         dbpy.SQL_BULK_COPY(em.server, em.database_name, 'dbo.ROAD_DETAIL') as sbc:

        for enum, (pfi, geom) in enumerate(sc):
            sbc.add_row((pfi, geom.length, geom.pointCount - 1))
            if enum % 10000 == 0:
                logging.info(enum)
        logging.info(enum)

    logging.info('count start: {}'.format(sbc.count_start))
    logging.info('count start: {}'.format(sbc.count_finish))
    

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

                create_road_detail_table(estamap_version)
                calc_road_detail(estamap_version)

            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')
