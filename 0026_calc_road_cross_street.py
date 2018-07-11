'''
Calculates ROAD TURN between all road segments at each intersection.

Usage:
  calc_road_xstreet.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  --temp_lmdb <lmdb>      LMDB location [default: c:\\temp\\road_xstreet]
  --log_file <file>       Log File name. [default: calc_road_xstreet.log]
  --log_path <folder>     Folder to store the log file. [default: c:\\temp]


ROAD_XSTREET_VALIDATION.NODE_TYPE = 'FROM' AND ( FROM_differences.ROAD_RNID <> 1312) AND (( FROM_differences.LEFT_LOCALITY not like '%(NSW)%' and FROM_differences.RIGHT_LOCALITY not like '%(NSW)%') and (FROM_differences.LEFT_LOCALITY not like '%(SA)%' and FROM_differences.RIGHT_LOCALITY not like '%(SA)%'))
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
import shapely.wkb
import shapely.geometry
import shapely.ops
import pandas as pd

import log
import dev as gis
import dbpy


def create_road_xstreet_table(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    sql_script = os.path.join(em.path, 'sql', 'transport', 'create_road_xstreet.sql')
    logging.info('running sql script: {}'.format(sql_script))

    dbpy.exec_script(em.server, em.database_name, sql_script)


def create_road_xstreet_traversal_table(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    sql_script = os.path.join(em.path, 'sql', 'transport', 'create_road_xstreet_traversal.sql')
    logging.info('running sql script: {}'.format(sql_script))

    dbpy.exec_script(em.server, em.database_name, sql_script)
   

def calc_road_xstreet(estamap_version,
                      temp_lmdb='c:\\temp\\road_xstreet',
                      temp_traversal_lmdb='c:\\temp\\road_xstreet_traversal'):

    logging.info('environment')
    em = gis.ESTAMAP('DEV')

    logging.info('create temp fgdb for ROAD_XSTREET_VALIDATION')
    if arcpy.Exists(os.path.join(r'c:\temp\road_xstreet_validation.gdb')):
        arcpy.Delete_management(r'c:\temp\road_xstreet_validation.gdb')
    
    arcpy.CreateFileGDB_management(out_folder_path=r'c:\temp',
                                   out_name='road_xstreet_validation.gdb')
    
    arcpy.CreateFeatureclass_management(out_path=r'c:\temp\road_xstreet_validation.gdb',
                                        out_name='ROAD_XSTREET_VALIDATION',
                                        geometry_type='POLYLINE',
                                        spatial_reference=arcpy.SpatialReference(3111))

    arcpy.AddField_management(in_table=os.path.join(r'c:\temp\road_xstreet_validation.gdb', 'ROAD_XSTREET_VALIDATION'),
                              field_name='PFI',
                              field_type='LONG')
    arcpy.AddField_management(in_table=os.path.join(r'c:\temp\road_xstreet_validation.gdb', 'ROAD_XSTREET_VALIDATION'),
                              field_name='NODE_TYPE',
                              field_type='TEXT',
                              field_length=4)
    arcpy.AddField_management(in_table=os.path.join(r'c:\temp\road_xstreet_validation.gdb', 'ROAD_XSTREET_VALIDATION'),
                              field_name='TRAVERSAL_DIST',
                              field_type='FLOAT',
                              field_precision=12,
                              field_scale=3)

    logging.info('create temp fgdb for ROAD_XSTREET_ROAD')
    if arcpy.Exists(os.path.join(r'c:\temp\road_xstreet_road.gdb')):
        arcpy.Delete_management(r'c:\temp\road_xstreet_road.gdb')
    
    arcpy.CreateFileGDB_management(out_folder_path=r'c:\temp',
                                   out_name='road_xstreet_road.gdb')
    
    arcpy.CreateFeatureclass_management(out_path=r'c:\temp\road_xstreet_road.gdb',
                                        out_name='ROAD_XSTREET_ROAD',
                                        geometry_type='POLYLINE',
                                        spatial_reference=arcpy.SpatialReference(3111))
    arcpy.AddField_management(in_table=os.path.join(r'c:\temp\road_xstreet_road.gdb', 'ROAD_XSTREET_ROAD'),
                              field_name='PFI',
                              field_type='LONG')
    arcpy.AddField_management(in_table=os.path.join(r'c:\temp\road_xstreet_road.gdb', 'ROAD_XSTREET_ROAD'),
                              field_name='NODE_TYPE',
                              field_type='TEXT',
                              field_length=4)
    arcpy.AddField_management(in_table=os.path.join(r'c:\temp\road_xstreet_road.gdb', 'ROAD_XSTREET_ROAD'),
                              field_name='XSTREET_PFI',
                              field_type='LONG')


    logging.info('creating temp lmdb: {}'.format(temp_lmdb))
    
    if os.path.exists(temp_lmdb):
        shutil.rmtree(temp_lmdb)
    env = lmdb.Environment(path=temp_lmdb,
                           map_size=1500000000,
                           readonly=False,
                           max_dbs=10)
    road_db = env.open_db('road', dupsort=True)
    road_geom_db = env.open_db('road_geom')
    road_bearing_db = env.open_db('road_bearing', dupsort=True)
    road_turn_db = env.open_db('road_turn', dupsort=True)
    road_alias_db = env.open_db('road_alias', dupsort=True)
    road_infrastructure_db = env.open_db('road_infrastructure', dupsort=True)
    

    logging.info('read ROAD')
    with env.begin(write=True, db=road_db) as txn, \
         arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD'),
                               field_names=['PFI', 'FROM_UFI', 'TO_UFI', 'FEATURE_TYPE_CODE']) as sc:
        for enum, row in enumerate(sc):
            txn.put(str(row[0]), ','.join([str(_) for _ in row[1:]]))
            if enum % 100000 == 0:
                logging.info(enum)
        logging.info(enum)
        
    logging.info('read ROAD geom')
    with env.begin(write=True, db=road_geom_db) as txn, \
         arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD'),
                               field_names=['PFI', 'SHAPE@WKB']) as sc:
        for enum, (pfi, wkb) in enumerate(sc):
            txn.put(str(pfi), str(wkb))
            if enum % 100000 == 0:
                logging.info(enum)
        logging.info(enum)
    
    logging.info('read ROAD_BEARING')
    with env.begin(write=True, db=road_bearing_db) as txn, \
         arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD_BEARING'),
                               field_names=['PFI',
                                            'ENTRY_BEARING',
                                            'EXIT_BEARING',
                                            'ENTRY_BEARING_FLIP',
                                            'EXIT_BEARING_FLIP']) as sc:

        for enum, (pfi, entry_bear, exit_bear, entry_bear_flip, exit_bear_flip) in enumerate(sc):

            pfi = str(pfi)
            txn.put(pfi+'ENTRY', '{:.5f}'.format(entry_bear))
            txn.put(pfi+'EXIT', '{:.5f}'.format(exit_bear))
            txn.put(pfi+'ENTRY_FLIP', '{:.5f}'.format(entry_bear_flip))
            txn.put(pfi+'EXIT_FLIP', '{:.5f}'.format(exit_bear_flip))

            if enum % 100000 == 0:
                logging.info(enum)
        logging.info(enum)

    logging.info('read ROAD_TURN')
    with env.begin(write=True, db=road_turn_db) as txn, \
         arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD_TURN'),
                               field_names=['UFI',
                                            'FROM_PFI',
                                            'TO_PFI',
                                            'ANGLE',
                                            'FROM_BEARING',
                                            'TO_BEARING']) as sc:
            
        for enum, (ufi, from_pfi, to_pfi, angle, from_bearing, to_bearing) in enumerate(sc):

            txn.put(str(ufi), ','.join([str(o) for o in (from_pfi, to_pfi, angle, from_bearing, to_bearing)]))
            
            if enum % 100000 == 0:
                logging.info(enum)
        logging.info(enum)

    logging.info('read ROAD_ALIAS')
    with env.begin(write=True, db=road_alias_db) as txn, \
         arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD_ALIAS'),
                               field_names=['PFI', 'ROAD_NAME_ID', 'ALIAS_NUMBER']) as sc:
        for enum, (pfi, rnid, alias_num) in enumerate(sc):
            txn.put(str(pfi), str(rnid) + ',' + str(alias_num))
            if enum % 100000 == 0:
                logging.info(enum)
        logging.info(enum)

    logging.info('read ROAD_INFRASTRUCTURE')
    with env.begin(write=True, db=road_infrastructure_db) as txn, \
         arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD_INFRASTRUCTURE'),
                               field_names=['UFI', 'FEATURE_TYPE_CODE']) as sc:

        for enum, (ufi, ftc) in enumerate(sc):
            txn.put(str(ufi), str(ftc))
            if enum % 100000 == 0:
                logging.info(enum)
        logging.info(enum)

    ##############
    logging.info('preparation')
    with env.begin(db=road_db) as road_txn, \
         env.begin(db=road_turn_db) as road_turn_txn, \
         env.begin(db=road_alias_db) as road_alias_txn, \
         env.begin(db=road_infrastructure_db) as road_infrastructure_txn, \
         env.begin(db=road_geom_db) as road_geom_txn, \
         dbpy.SQL_BULK_COPY(em.server, em.database_name, 'dbo.ROAD_XSTREET') as sbc_xstreet, \
         dbpy.SQL_BULK_COPY(em.server, em.database_name, 'dbo.ROAD_XSTREET_TRAVERSAL') as sbc_xstreet_traversal:
        
        road_cursor = road_txn.cursor()
        road_geom_cursor = road_geom_txn.cursor()
        road_cursor_iter = road_txn.cursor()
        road_turn_cursor = road_turn_txn.cursor()
        road_alias_cursor = road_alias_txn.cursor()
        road_infrastructure_cursor = road_infrastructure_txn.cursor()
        road_infrastructure_cursor_iter = road_infrastructure_txn.cursor()
        

        # convienience functions
        get_road_nodes_cursor = road_txn.cursor()
        def get_road_nodes(pfi):
            return get_road_nodes_cursor.get(pfi).split(',')[:-1]
        
        get_road_rnids_cursor = road_alias_txn.cursor()
        def get_road_rnids(pfi):
            rnids = []
            get_road_rnids_cursor.set_key(pfi)
            for values in get_road_rnids_cursor.iternext_dup():
                rnid, alias_num = values.split(',')
                rnids.append((rnid, alias_num))
            return sorted(rnids, key=lambda x: x[-1])

        get_road_ftc_cursor = road_txn.cursor()
        def get_road_ftc(pfi):
            return get_road_ftc_cursor.get(pfi).split(',')[-1]

        get_connecting_pfis_rt_cursor = road_turn_txn.cursor()
        get_connecting_pfis_ri_cursor = road_infrastructure_txn.cursor()
        def get_connecting_pfis(ufi, pfi):
            connecting_pfis = []
            get_connecting_pfis_rt_cursor.set_key(ufi)
            for values in get_connecting_pfis_rt_cursor.iternext_dup():
                from_pfi, to_pfi, angle, from_bearing, to_bearing = values.split(',')
                if from_pfi == pfi:
                    connecting_pfis.append([to_pfi, angle])
            return sorted(connecting_pfis, key=lambda x: abs(float(x[-1])))

        get_road_altnode_cursor = road_txn.cursor()
        def get_road_altnode(pfi, current_node):
            from_ufi, to_ufi, pfi_ftc = get_road_altnode_cursor.get(pfi).split(',')
            if current_node == from_ufi:
                return to_ufi
            else:
                return from_ufi

        def get_traversal(pfi, ufi):
            traversal_pfis = get_connecting_pfis(ufi, pfi)
            traversal_pfis_sort_180 = sorted(traversal_pfis, key=lambda x: abs(180 - abs(float(x[-1]))))
                            
            if len(traversal_pfis) == 0:
                # no roads connecting
                return 'ROAD_END', None, None

            else:
                # determine best traversal
                
                pfi_rnid = get_road_rnids(pfi)[0][0]
                
                # 1. road has SAME_RNID and PFI is not UNNAMED
                if pfi_rnid <> '1312':
                    for con_pfi, con_angle in traversal_pfis_sort_180:
                        con_pfi_rnids = get_road_rnids(con_pfi)
                        if pfi_rnid in [rnid for rnid, an in con_pfi_rnids]:
                            traversal_desc = 'SAME_RNID'
                            traversal_pfi = con_pfi
                            traversal_ufi = get_road_altnode(con_pfi, ufi)
                            return 'SAME_RNID', con_pfi, get_road_altnode(con_pfi, ufi)

                # 2. road angle closest to 180 degrees
##                traversal_pfis_sort_180 = sorted(traversal_pfis, key=lambda x: abs(180 - abs(float(x[-1]))))
                traversal_pfi = traversal_pfis_sort_180[0][0]                
                return 'CLOSE_TO_180', traversal_pfi, get_road_altnode(traversal_pfi, ufi)

        def process_node(pfi, ufi, node_type):
            
            pfi_rnid = get_road_rnids(pfi)[0][0]   # get PFI RNID (primary rnid)
            pfi_from_ufi, pfi_to_ufi = get_road_nodes(pfi)
            
            traversal = []
            xstreet = []
            traversal_order = 0
            traversal_desc = 'BEGIN'
            traversal_pfi = pfi
            if node_type == 'FROM':
                traversal_ufi = pfi_from_ufi
            else:
                traversal_ufi = pfi_to_ufi
            xstreet_pfi = None
            xstreet_rnid = None
            
            while True:
                
                # get connecting PFI at FROM_UFI
                from_ufi_pfis = get_connecting_pfis(traversal_ufi, traversal_pfi)

                traversal.append([pfi, node_type, traversal_order, traversal_pfi, traversal_ufi, len(from_ufi_pfis), traversal_desc])

                if len(from_ufi_pfis) == 0:
                    traversal_desc = 'ROAD_END'
                    break

                # determine if suitable XSTREET
                for from_ufi_pfi, from_ufi_angle in from_ufi_pfis:
                    from_ufi_pfi_rnids = get_road_rnids(from_ufi_pfi)
                    from_ufi_pfi_ftc = get_road_ftc(from_ufi_pfi)
                    from_ufi_pfi_rnids_only = [rnid for rnid, an in from_ufi_pfi_rnids]
                    
                    if '1312' in from_ufi_pfi_rnids_only:
                        # road is UNNAMED
                        continue
                    if pfi_rnid in from_ufi_pfi_rnids_only:
                        # road has same RNID
                        continue
                    if from_ufi_pfi_ftc == 'TUNNEL':
                        # road type is a TUNNEL
                        continue
                    xstreet_pfi = from_ufi_pfi
                    xstreet_rnid = from_ufi_pfi_rnids[0][0]
                    traversal_desc = 'XSTREET'
                    break
                if traversal_desc == 'XSTREET':
                    traversal.append([pfi, node_type, traversal_order, traversal_pfi, traversal_ufi, len(from_ufi_pfis), traversal_desc])
                    break
                
                # determine next suitable traversal if XSTREET not found
                traversal_desc, traversal_pfi, traversal_ufi = get_traversal(traversal_pfi, traversal_ufi)


                # add loop check here

                
                if traversal_order > 50:
                    # exit if traversal too long
                    traversal_desc = 'MORE_THAN_50'
                    break
                
                traversal_order = traversal_order + 1

##            traversal.append([pfi, node_type, traversal_order, traversal_pfi, traversal_ufi, len(from_ufi_pfis), traversal_desc])
            
            return xstreet_pfi, xstreet_rnid, traversal


        
        with arcpy.da.InsertCursor(in_table=os.path.join(r'c:\temp\road_xstreet_validation.gdb', 'ROAD_XSTREET_VALIDATION'),
                                   field_names=['PFI', 'NODE_TYPE', 'TRAVERSAL_DIST', 'SHAPE@WKB']) as ic_valid, \
             arcpy.da.InsertCursor(in_table=os.path.join(r'c:\temp\road_xstreet_road.gdb', 'ROAD_XSTREET_ROAD'),
                                   field_names=['PFI', 'NODE_TYPE', 'XSTREET_PFI', 'SHAPE@WKB']) as ic_road:

            logging.info('looping roads')
            for enum_road, pfi in enumerate(road_cursor.iternext(keys=True, values=False)):

                # get PFI RNID (primary rnid)
                pfi_rnid = get_road_rnids(pfi)[0][0]
                pfi_from_ufi, pfi_to_ufi = get_road_nodes(pfi)
                
                from_xstreet_pfi, from_xstreet_rnid, from_traversal = process_node(pfi, pfi_from_ufi, 'FROM')
                to_xstreet_pfi, to_xstreet_rnid, to_traversal = process_node(pfi, pfi_to_ufi, 'TO')
                
                #
                # insert FROM traversal
                #
                for f_traversal in from_traversal:
                    sbc_xstreet_traversal.add_row(f_traversal)

                from_geoms = []
                for f_traversal in from_traversal:
                    from_geoms.append(shapely.wkb.loads(road_geom_cursor.get(f_traversal[3])))
                    
                from_merged_line = shapely.ops.linemerge(from_geoms)
                # measure actual traversal distance (subtract base road length)
                from_traversal_dist = from_merged_line.length - shapely.wkb.loads(road_geom_cursor.get(pfi)).length
                
                if from_xstreet_pfi:

                    # (subtract xstreet road length)
##                    from_traversal_dist = from_traversal_dist - shapely.wkb.loads(road_geom_cursor.get(from_xstreet_pfi)).length

                    # add the xstreet geom
                    from_xstreet_geom = shapely.wkb.loads(road_geom_cursor.get(from_xstreet_pfi))
                    from_geoms.append(from_xstreet_geom)

                    # insert into ROAD_XSTREET_ROAD
                    ic_road.insertRow([pfi, 'FROM', from_xstreet_pfi, shapely.wkb.loads(road_geom_cursor.get(from_xstreet_pfi)).wkb])
                    
                from_merged_line_final = shapely.ops.linemerge(from_geoms)
                
                ic_valid.insertRow([pfi, 'FROM', from_traversal_dist, from_merged_line_final.wkb])
                ##

                #
                # insert TO traversal
                #
                for t_traversal in to_traversal:
                    sbc_xstreet_traversal.add_row(t_traversal)

                to_geoms = []
                for t_traversal in to_traversal:
                    to_geoms.append(shapely.wkb.loads(road_geom_cursor.get(t_traversal[3])))
                    
                to_merged_line = shapely.ops.linemerge(to_geoms)
                # measure actual traversal distance (subtract base road)
                to_traversal_dist = to_merged_line.length - shapely.wkb.loads(road_geom_cursor.get(pfi)).length
                
                if to_xstreet_pfi:

                    # (subtract xstreet road length)
##                    to_traversal_dist = to_traversal_dist - shapely.wkb.loads(road_geom_cursor.get(to_xstreet_pfi)).length

                    # add the xstreet geom
                    to_xstreet_geom = shapely.wkb.loads(road_geom_cursor.get(to_xstreet_pfi))
                    to_geoms.append(to_xstreet_geom)

                    # insert into ROAD_XSTREET_ROAD
                    ic_road.insertRow([pfi, 'TO', to_xstreet_pfi, shapely.wkb.loads(road_geom_cursor.get(to_xstreet_pfi)).wkb])
                    
                to_merged_line_final = shapely.ops.linemerge(to_geoms)

                ic_valid.insertRow([pfi, 'TO', to_traversal_dist, to_merged_line_final.wkb])
                ##

                sbc_xstreet.add_row([pfi, pfi_rnid,
                                     pfi_from_ufi, from_xstreet_rnid, from_xstreet_pfi,
                                     pfi_to_ufi, to_xstreet_rnid, to_xstreet_pfi])
                   
                if enum_road % 10000 == 0:
                    logging.info(enum_road)
                    sbc_xstreet.flush()
                    sbc_xstreet_traversal.flush()

            logging.info(enum_road)

        logging.info('indexes')
        arcpy.AddIndex_management(in_table=os.path.join(r'c:\temp\road_xstreet_validation.gdb', 'ROAD_XSTREET_VALIDATION'),
                                  fields='PFI',
                                  index_name='PFI',
                                  ascending=True)
        arcpy.AddIndex_management(in_table=os.path.join(r'c:\temp\road_xstreet_validation.gdb', 'ROAD_XSTREET_VALIDATION'),
                                  fields='NODE_TYPE',
                                  index_name='NODE')
        arcpy.AddIndex_management(in_table=os.path.join(r'c:\temp\road_xstreet_validation.gdb', 'ROAD_XSTREET_VALIDATION'),
                                  fields='TRAVERSAL_DIST',
                                  index_name='DIST')
        
        arcpy.AddIndex_management(in_table=os.path.join(r'c:\temp\road_xstreet_road.gdb', 'ROAD_XSTREET_ROAD'),
                                  fields='PFI',
                                  index_name='PFI',
                                  ascending=True)
        arcpy.AddIndex_management(in_table=os.path.join(r'c:\temp\road_xstreet_road.gdb', 'ROAD_XSTREET_ROAD'),
                                  fields='XSTREET_PFI',
                                  index_name='XPFI',
                                  ascending=True)

        logging.info('spatial indexes')
        arcpy.RemoveSpatialIndex_management(in_features=os.path.join(r'c:\temp\road_xstreet_validation.gdb', 'ROAD_XSTREET_VALIDATION'))
        arcpy.AddSpatialIndex_management(in_features=os.path.join(r'c:\temp\road_xstreet_validation.gdb', 'ROAD_XSTREET_VALIDATION'))
        arcpy.RemoveSpatialIndex_management(in_features=os.path.join(r'c:\temp\road_xstreet_road.gdb', 'ROAD_XSTREET_ROAD'))
        arcpy.AddSpatialIndex_management(in_features=os.path.join(r'c:\temp\road_xstreet_road.gdb', 'ROAD_XSTREET_ROAD'))


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

                create_road_xstreet_table(estamap_version)
                create_road_xstreet_traversal_table(estamap_version)
                calc_road_xstreet(estamap_version)
                
            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')
