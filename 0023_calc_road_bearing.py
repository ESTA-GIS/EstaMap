'''
Calculates ENTRY/EXIT bearing and FLIPPED bearing of ROAD.
 
Usage:
  calc_road_bearings.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  --log_file <file>       Log File name. [default: calc_road_bearings.log]
  --log_path <folder>     Folder to store the log file. [default: c:\\temp]
'''
import os
import sys
import time
import logging
import collections

from docopt import docopt
import shapely.geometry
import shapely.wkb
import numpy as np
import arcpy

import log
import dev as gis
import dbpy


def create_road_bearing_table(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    sql_script = os.path.join(em.path, 'sql', 'transport', 'create_road_bearing.sql')
    logging.info('running sql script: {}'.format(sql_script))

    dbpy.exec_script(em.server, em.database_name, sql_script)


def calc_road_bearing(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    cursor = em.conn.cursor()

    logging.info('get coords')
    pfis = []
    x_entry = []
    y_entry = []
    x_exit = []
    y_exit = []
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD'),
                               field_names=['PFI', 'SHAPE@WKB'],
                               spatial_reference=gis.ingr_spatial_reference(),
                               sql_clause=(None, 'ORDER BY PFI')) as sc:
        for enum, (pfi, geom_wkb) in enumerate(sc, 1):

            geom = shapely.wkb.loads(str(geom_wkb))

            first_point = geom.coords[0]
            entry_point = geom.interpolate(2.5)
            dx_entry = entry_point.x - first_point[0]
            dy_entry = entry_point.y - first_point[1]

            last_point = geom.coords[-1]
            exit_point = geom.interpolate(geom.length - 2.5)
            dx_exit = last_point[0] - exit_point.x
            dy_exit = last_point[1] - exit_point.y

            pfis.append(pfi)
            x_entry.append(dx_entry)
            y_entry.append(dy_entry)
            x_exit.append(dx_exit)
            y_exit.append(dy_exit)

            if enum % 10000 == 0:
                logging.info('{}'.format(enum))
        logging.info('{}'.format(enum))

    logging.info('calc bearings and insert')
    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'dbo.ROAD_BEARING') as sbc:
        
        for enum, (pfi, entry_angle, exit_angle) in enumerate(zip(pfis,
                                                                  np.arctan2(np.array(y_entry), np.array(x_entry)) * 180 / np.pi,
                                                                  np.arctan2(np.array(y_exit), np.array(x_exit)) * 180 / np.pi)):
##            if entry_angle < 0:
##                entry_bearing = (90 - entry_angle) % 360
##            else:
##                entry_bearing = (450 - entry_angle) % 360
##            if exit_angle < 0:
##                exit_bearing = (90 - exit_angle) % 360
##            else:
##                exit_bearing = (450 - exit_angle) % 360

##            if entry_angle < 0:
##                entry_bearing = int((90 - entry_angle) % 360)
##            else:
##                entry_bearing = int((450 - entry_angle) % 360)
##            if exit_angle < 0:
##                exit_bearing = int((90 - exit_angle) % 360)
##            else:
##                exit_bearing = int((450 - exit_angle) % 360)

            if entry_angle < 0:
                entry_bearing = float((90 - entry_angle) % 360)
            else:
                entry_bearing = float((450 - entry_angle) % 360)
            if exit_angle < 0:
                exit_bearing = float((90 - exit_angle) % 360)
            else:
                exit_bearing = float((450 - exit_angle) % 360)

            sbc.add_row((pfi, entry_bearing, exit_bearing, (exit_bearing + 180) % 360, (entry_bearing + 180) % 360))
            if enum % 10000 == 0:
                logging.info(enum)
                sbc.flush()
        logging.info(enum)
    logging.info('count start: {}'.format(sbc.count_start))
    logging.info('count finish: {}'.format(sbc.count_finish))


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
                
                create_road_bearing_table(estamap_version)
                calc_road_bearing(estamap_version)
                
            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')
