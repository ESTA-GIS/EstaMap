'''
Calculates ROAD TURN between all road segments at each intersection.

Usage:
  calc_road_turn.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  --temp_lmdb <lmdb>      LMDB location [default: c:\\temp\\turnangles_lmdb]
  --log_file <file>       Log File name. [default: calc_road_turn_angles.log]
  --log_path <folder>     Folder to store the log file. [default: c:\\temp]
'''
import os
import sys
import time
import logging
import itertools
import shutil

from docopt import docopt
import arcpy
import lmdb

import log
import dev as gis
import dbpy


def create_road_turn_table(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    sql_script = os.path.join(em.path, 'sql', 'transport', 'create_road_turn.sql')
    logging.info('running sql script: {}'.format(sql_script))

    dbpy.exec_script(em.server, em.database_name, sql_script)


def calc_road_turn(estamap_version, temp_lmdb='C:\\temp\\turnangles_lmdb'):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    cursor = em.conn.cursor()
    
    logging.info('create lmdb db')
    if os.path.exists(temp_lmdb):
        shutil.rmtree(temp_lmdb)
    env = lmdb.Environment(path=temp_lmdb,
                           map_size=1000000000,
                           readonly=False,
                           max_dbs=4)
    road_bearings_db = env.open_db('road_bearings', dupsort=True)
    road_pfis_db = env.open_db('road_pfis', dupsort=True)
    roads_at_ufi_db = env.open_db('roads_at_ufi', dupsort=True)
    road_infrastructure_db = env.open_db('road_infrastructure')


    logging.info('read bearings')
    with env.begin(write=True, db=road_bearings_db) as road_bearings_txn, \
         arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD_BEARING'),
                               field_names=['PFI',
                                            'ENTRY_BEARING',
                                            'EXIT_BEARING',
                                            'ENTRY_BEARING_FLIP',
                                            'EXIT_BEARING_FLIP']) as sc:

        for enum, (pfi, entry_bear, exit_bear, entry_bear_flip, exit_bear_flip) in enumerate(sc, 1):

            pfi = str(pfi)
            road_bearings_txn.put(pfi+'ENTRY', '{:.5f}'.format(entry_bear))
            road_bearings_txn.put(pfi+'EXIT', '{:.5f}'.format(exit_bear))
            road_bearings_txn.put(pfi+'ENTRY_FLIP', '{:.5f}'.format(entry_bear_flip))
            road_bearings_txn.put(pfi+'EXIT_FLIP', '{:.5f}'.format(exit_bear_flip))

            if enum % 10000 == 0:
                logging.info(enum)

    logging.info('read roads')
    with env.begin(write=True, db=road_pfis_db) as road_pfis_txn, \
         arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD'),
                               field_names=['PFI', 'FROM_UFI', 'TO_UFI']) as sc:

        for enum, (pfi, from_ufi, to_ufi) in enumerate(sc, 1):
            road_pfis_txn.put(str(pfi), str(from_ufi) + ',' + str(to_ufi))
            if enum % 10000 == 0:
                logging.info(enum)

                
    logging.info('read road_infrastructure')
    with env.begin(write=True, db=road_infrastructure_db) as road_infrastructure_txn, \
         arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD_INFRASTRUCTURE'),
                               field_names=['UFI', 'FEATURE_TYPE_CODE', 'CONPFI1', 'CONPFI2']) as sc:

        for enum, (ufi, ftc, conpfi1, conpfi2) in enumerate(sc, 1):
            road_infrastructure_txn.put(str(ufi), str(ftc) + ',' + str(conpfi1) + ',' + str(conpfi2))
            if enum % 10000 == 0:
                logging.info(enum)


    logging.info('find roads at ufi')
    with env.begin(db=road_pfis_db) as road_pfis_txn, \
         env.begin(write=True, db=roads_at_ufi_db) as roads_at_ufi_txn:

        cursor_road = road_pfis_txn.cursor()

        for enum_pfi, (pfi, ufis) in enumerate(cursor_road.iternext(), 1):
            
            from_ufi, to_ufi = ufis.split(',')

            roads_at_ufi_txn.put(from_ufi, pfi)
            roads_at_ufi_txn.put(to_ufi, pfi)

    logging.info('iterating permutations and insert')
    with env.begin(db=roads_at_ufi_db) as roads_at_ufi_txn, \
         env.begin(db=road_bearings_db) as road_bearings_txn, \
         env.begin(db=road_pfis_db) as road_pfis_txn, \
         env.begin(db=road_infrastructure_db) as road_infrastructure_txn, \
         dbpy.SQL_BULK_COPY(em.server, em.database_name, 'dbo.ROAD_TURN') as sbc:

        cursor_keys = roads_at_ufi_txn.cursor()
        cursor_values = roads_at_ufi_txn.cursor()
        road_infrastructure_cursor = road_infrastructure_txn.cursor()

        pfis_at_ufi = set()
        for enum_ufi, ufi in enumerate(cursor_keys.iternext_nodup(), 1):
            
            try:
                ufi_ftc, ufi_conpfi1, ufi_conpfi2 = road_infrastructure_cursor.get(ufi).split(',')
            except:
                print '    ', ufi
                continue

##            if ufi == '51580977':
##                print road_infrastructure_cursor.get(ufi)
##                print ufi_conpfi1, ufi_conpfi2
            
            cursor_values.set_key(ufi)
            
            pfis_at_ufi.clear()
            for pfi in cursor_values.iternext_dup():
                pfis_at_ufi.add(pfi)

            # compute turn angles only if more than one unique road segment at ufi
            if len(pfis_at_ufi) > 1:

                for from_pfi, to_pfi in itertools.permutations(pfis_at_ufi, 2):

                    # check if valid turn
##                    if ufi == '51580977':
##                        print from_pfi, to_pfi
                    
                    if ufi_ftc.lower() == 'tunnel':
                        if from_pfi in (ufi_conpfi1, ufi_conpfi2):
                            if to_pfi not in (ufi_conpfi1, ufi_conpfi2):
                                continue
                        if to_pfi in (ufi_conpfi1, ufi_conpfi2):
                            if from_pfi not in (ufi_conpfi1, ufi_conpfi2):
                                continue

                    # check if current ufi is same as from_pfi's from_ufi
                    if ufi == road_pfis_txn.get(from_pfi).split(',')[0]:
                        from_bearing = float(road_bearings_txn.get(from_pfi+'ENTRY'))
                    else:
                        from_bearing = float(road_bearings_txn.get(from_pfi+'ENTRY_FLIP'))

                    # check if current ufi is same as to_pfi's from_ufi
                    if ufi == road_pfis_txn.get(to_pfi).split(',')[0]:
                        to_bearing = float(road_bearings_txn.get(to_pfi+'ENTRY'))
                    else:
                        to_bearing = float(road_bearings_txn.get(to_pfi+'ENTRY_FLIP'))

                    angle = from_bearing - to_bearing
                    if angle < -180:
                        angle = angle + 360
                    elif angle > 180:
                        angle = angle - 360

                    sbc.add_row((ufi, from_pfi, to_pfi, angle, from_bearing, to_bearing))

            if enum_ufi % 10000 == 0:
                logging.info(enum_ufi)
                sbc.flush()
        logging.info(enum_ufi)

    logging.info('count start: {}'.format(sbc.count_start))
    logging.info('count finish: {}'.format(sbc.count_finish))


if __name__ == '__main__':

    sys.argv.append('--estamap_version=DEV')

    with log.LogConsole():
        
        logging.info('parsing args')
        args = docopt(__doc__)

        logging.info('variables')
        estamap_version = args['--estamap_version']
        temp_lmdb = args['--temp_lmdb']
        log_file = args['--log_file']
        log_path = args['--log_path']

        with log.LogFile(log_file, log_path):
            logging.info('start')
            try:

                create_road_turn_table(estamap_version)
                calc_road_turn(estamap_version, temp_lmdb)

            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')
