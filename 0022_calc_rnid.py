'''
Transport:
Creates Road Name ID from TR_ROAD_NAME and TR_ROAD_TYPE.

Usage:
  calc_rnid.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  --log_file <file>       Log File name. [default: create_rnid_transport.log]
  --log_path <folder>     Folder to store the log file. [default: c:\\temp]

'''
import os
import sys
import time
import math
import logging
import re

from docopt import docopt
import arcpy
import pandas as pd

import log
import dev as gis
import dbpy


def register_new_road_roadname(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    cursor = em.conn.cursor()

    results = cursor.execute('select count(*) from ROAD_NAME_REGISTER').fetchval()
    logging.info('total ROAD_NAME_REGISTER: {}'.format(results))

    sql_stmt = '''
    select distinct road_name   as ROAD_NAME, road_type   as ROAD_TYPE, road_suffix   as ROAD_SUFFIX from dbo.ROAD where road_name   is not null union 
    select distinct road_name_1 as ROAD_NAME, road_type_1 as ROAD_TYPE, road_suffix_1 as ROAD_SUFFIX from dbo.ROAD where road_name_1 is not null union 
    select distinct road_name_2 as ROAD_NAME, road_type_2 as ROAD_TYPE, road_suffix_2 as ROAD_SUFFIX from dbo.ROAD where road_name_2 is not null union 
    select distinct road_name_3 as ROAD_NAME, road_type_3 as ROAD_TYPE, road_suffix_3 as ROAD_SUFFIX from dbo.ROAD where road_name_3 is not null union 
    select distinct road_name_4 as ROAD_NAME, road_type_4 as ROAD_TYPE, road_suffix_4 as ROAD_SUFFIX from dbo.ROAD where road_name_4 is not null union 
    select distinct road_name_5 as ROAD_NAME, road_type_5 as ROAD_TYPE, road_suffix_5 as ROAD_SUFFIX from dbo.ROAD where road_name_5 is not null union 
    select distinct road_name_6 as ROAD_NAME, road_type_6 as ROAD_TYPE, road_suffix_6 as ROAD_SUFFIX from dbo.ROAD where road_name_6 is not null union 
    select distinct road_name_7 as ROAD_NAME, road_type_7 as ROAD_TYPE, road_suffix_7 as ROAD_SUFFIX from dbo.ROAD where road_name_7 is not null
    order by ROAD_NAME'''

    new_roadnames = []
    route_flag = 0
    for enum, (road_name, road_type, road_suffix) in enumerate(cursor.execute(sql_stmt), 1):

        # parse roadname
        road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag = em.parse_roadname(road_name, road_type, road_suffix, route_flag=route_flag)

        if not em.check_roadname(road_name=road_name_parsed, road_type=road_type_parsed, road_suffix=road_suffix_parsed):
            new_roadnames.append((road_name_parsed, road_type_parsed, road_suffix_parsed))
        em.create_roadname(road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag)

    logging.info('Num New Road Names: {}'.format(len(new_roadnames)))        
    
    results = cursor.execute('select count(*) from ROAD_NAME_REGISTER').fetchval()
    logging.info('total ROAD_NAME_REGISTER: {}'.format(results)) 


def register_new_road_routeno(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    cursor = em.conn.cursor()

    results = cursor.execute('select count(*) from ROAD_NAME_REGISTER').fetchval()
    logging.info('total ROAD_NAME_REGISTER: {}'.format(results))

    sql_stmt = '''
    select distinct route_no from dbo.ROAD where route_no is not null
    order by route_no
    '''

    new_routenos = []
    for enum, (route_no,) in enumerate(cursor.execute(sql_stmt), 1):

        # parse route_no
        road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag = em.parse_route_name(route_no)

        if not em.check_roadname(road_name=road_name_parsed, road_type=road_type_parsed, road_suffix=road_suffix_parsed):
            new_routenos.append((road_name_parsed, road_type_parsed, road_suffix_parsed))
        em.create_roadname(road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag)

    logging.info('Num New Road Names: {}'.format(len(new_routenos)))        
    
    results = cursor.execute('select count(*) from ROAD_NAME_REGISTER').fetchval()
    logging.info('total ROAD_NAME_REGISTER: {}'.format(results))


def register_new_road_structurename(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    cursor = em.conn.cursor()

    results = cursor.execute('select count(*) from ROAD_NAME_REGISTER').fetchval()
    logging.info('total ROAD_NAME_REGISTER: {}'.format(results))

    sql_stmt = '''
    select distinct structure_name from dbo.ROAD where structure_name is not null
    order by structure_name
    '''

    new_structurenames = []
    for enum, (structure_name,) in enumerate(cursor.execute(sql_stmt), 1):

        # parse road name
        road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag = em.parse_roadname(structure_name, road_type='-', road_suffix='', route_flag=0)
        
        if not em.check_roadname(road_name=road_name_parsed, road_type=road_type_parsed, road_suffix=road_suffix_parsed):
            new_structurenames.append((road_name_parsed, road_type_parsed, road_suffix_parsed))
        em.create_roadname(road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag)

    logging.info('Num New Road Names: {}'.format(len(new_structurenames)))        
    
    results = cursor.execute('select count(*) from ROAD_NAME_REGISTER').fetchval()
    logging.info('total ROAD_NAME_REGISTER: {}'.format(results))   


def register_new_roadinfrastructure_name(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    cursor = em.conn.cursor()

    results = cursor.execute('select count(*) from ROAD_NAME_REGISTER').fetchval()
    logging.info('total ROAD_NAME_REGISTER: {}'.format(results))

    sql_stmt = '''
    select distinct NAME from dbo.ROAD_INFRASTRUCTURE where NAME is not null and NAME <> 'TEMP'
    order by NAME
    '''
    
    new_names = []
    for enum, (name,) in enumerate(cursor.execute(sql_stmt), 1):

        # parse road name
        road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag = em.parse_roadname(name, '-', '', 0)

        if not em.check_roadname(road_name=road_name_parsed, road_type=road_type_parsed, road_suffix=road_suffix_parsed):
            new_names.append((road_name_parsed, road_type_parsed, road_suffix_parsed))
        em.create_roadname(road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag)

    logging.info('Num New Road Names: {}'.format(len(new_names)))

    results = cursor.execute('select count(*) from ROAD_NAME_REGISTER').fetchval()
    logging.info('total ROAD_NAME_REGISTER: {}'.format(results))   


def create_road_alias_table(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    sql_script = os.path.join(em.path, 'sql', 'transport', 'create_road_alias.sql')
    logging.info('running sql script: {}'.format(sql_script))

    dbpy.exec_script(em.server, em.database_name, sql_script)

    
def calc_road_alias(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    cursor = em.conn.cursor()

    sql_stmt = '''
    select
        PFI,
        ROAD_NAME, ROAD_TYPE, ROAD_SUFFIX,
        ROAD_NAME_1, ROAD_TYPE_1, ROAD_SUFFIX_1,
        ROAD_NAME_2, ROAD_TYPE_2, ROAD_SUFFIX_2,
        ROAD_NAME_3, ROAD_TYPE_3, ROAD_SUFFIX_3,
        ROAD_NAME_4, ROAD_TYPE_4, ROAD_SUFFIX_4,
        ROAD_NAME_5, ROAD_TYPE_5, ROAD_SUFFIX_5,
        ROAD_NAME_6, ROAD_TYPE_6, ROAD_SUFFIX_6,
        ROAD_NAME_7, ROAD_TYPE_7, ROAD_SUFFIX_7,
        ROUTE_NO, STRUCTURE_NAME
    from ROAD
    '''
    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'dbo.ROAD_ALIAS') as sbc:
        
        for enum, row in enumerate(cursor.execute(sql_stmt)):
            pfi, road_name_0, road_type_0, road_suffix_0, \
                 road_name_1, road_type_1, road_suffix_1, \
                 road_name_2, road_type_2, road_suffix_2, \
                 road_name_3, road_type_3, road_suffix_3, \
                 road_name_4, road_type_4, road_suffix_4, \
                 road_name_5, road_type_5, road_suffix_5, \
                 road_name_6, road_type_6, road_suffix_6, \
                 road_name_7, road_type_7, road_suffix_7, \
                 route_no, structure_name = row
            rns = [(road_name_0, road_type_0, road_suffix_0, 0),
                   (road_name_1, road_type_1, road_suffix_1, 0),
                   (road_name_2, road_type_2, road_suffix_2, 0),
                   (road_name_3, road_type_3, road_suffix_3, 0),
                   (road_name_4, road_type_4, road_suffix_4, 0),
                   (road_name_5, road_type_5, road_suffix_5, 0),
                   (road_name_6, road_type_6, road_suffix_6, 0),
                   (road_name_7, road_type_7, road_suffix_7, 0),
                   (route_no, '-', '', 1),
                   (structure_name, '-', '', 0),
                   ]

            pfi_unique = set()
            for alias_num, (road_name, road_type, road_suffix, route_flag) in enumerate(rns):
                if not road_name:
                    continue

                # parse road_name
                if route_flag == 1:
                    road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag = em.parse_route_name(road_name)
                else:
                    road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag = em.parse_roadname(road_name, road_type, road_suffix, route_flag)

                # check parsed roadname
                roadname = em.check_roadname(road_name_parsed, road_type_parsed, road_suffix_parsed)
                if (pfi, roadname.ROAD_NAME_ID) not in pfi_unique:
                    pfi_unique.add((pfi, roadname.ROAD_NAME_ID))
                    
                    sbc.add_row((pfi, roadname.ROAD_NAME_ID, alias_num, route_flag))
                    
            if enum % 10000 == 0:
                logging.info(enum)
                sbc.flush()
        logging.info(enum)
    logging.info('count start: {}'.format(sbc.count_start))
    logging.info('count finish: {}'.format(sbc.count_finish))


def create_road_infrastructure_rnid_table(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    sql_script = os.path.join(em.path, 'sql', 'transport', 'create_road_infrastructure_rnid.sql')
    logging.info('running sql script: {}'.format(sql_script))

    dbpy.exec_script(em.server, em.database_name, sql_script)


def calc_road_infrastructure_rnid(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    cursor = em.conn.cursor()

    sql_stmt = '''
    select UFI, NAME
    from ROAD_INFRASTRUCTURE
    where NAME is not null and NAME <> 'TEMP'
    order by NAME
    '''
    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'dbo.ROAD_INFRASTRUCTURE_RNID') as sbc:
        
        for enum, (ufi, name) in enumerate(cursor.execute(sql_stmt)):

            # parse road name
            road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag = em.parse_roadname(name, '-', '', 0)

            # check parsed roadname
            roadname = em.check_roadname(road_name_parsed, road_type_parsed, road_suffix_parsed)

            sbc.add_row((ufi, roadname.ROAD_NAME_ID))

            if enum % 10000 == 0:
                logging.info(enum)
                sbc.flush()
        logging.info(enum)

    logging.info('count start: {}'.format(sbc.count_start))
    logging.info('count finish: {}'.format(sbc.count_finish))


################


def register_new_address_roadname(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    cursor = em.conn.cursor()

    results = cursor.execute('select count(*) from ROAD_NAME_REGISTER').fetchval()
    logging.info('total ROAD_NAME_REGISTER: {}'.format(results))

    sql_stmt = '''
    select distinct ROAD_NAME, ROAD_TYPE, ROAD_SUFFIX from ADDRESS
    '''
    
    new_roadnames = []
    route_flag = 0
    for enum, (road_name, road_type, road_suffix) in enumerate(cursor.execute(sql_stmt), 1):

        # parse roadname
        road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag = em.parse_roadname(road_name, road_type, road_suffix, route_flag=route_flag)

        if not em.check_roadname(road_name=road_name_parsed, road_type=road_type_parsed, road_suffix=road_suffix_parsed):
            new_roadnames.append((road_name_parsed, road_type_parsed, road_suffix_parsed))
        em.create_roadname(road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag)

    logging.info('Num New Road Names: {}'.format(len(new_roadnames)))        
    
    results = cursor.execute('select count(*) from ROAD_NAME_REGISTER').fetchval()
    logging.info('total ROAD_NAME_REGISTER: {}'.format(results)) 


def create_address_rnid_table(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    sql_script = os.path.join(em.path, 'sql', 'transport', 'create_address_rnid.sql')
    logging.info('running sql script: {}'.format(sql_script))

    dbpy.exec_script(em.server, em.database_name, sql_script)



def calc_address_rnid(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    cursor = em.conn.cursor()

    sql_stmt = '''
    select PFI, ROAD_NAME, ROAD_TYPE, ROAD_SUFFIX
    from ADDRESS
    '''
    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'dbo.ADDRESS_RNID') as sbc:

        logging.info('looping address')
        route_flag = 0
        for enum, (pfi, road_name, road_type, road_suffix) in enumerate(cursor.execute(sql_stmt)):

            road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag = em.parse_roadname(road_name, road_type, road_suffix, route_flag)

            # check parsed roadname
            roadname = em.check_roadname(road_name_parsed, road_type_parsed, road_suffix_parsed)
            
            sbc.add_row((pfi, roadname.ROAD_NAME_ID))
                
            if enum % 10000 == 0:
                logging.info(enum)
                sbc.flush()
        logging.info(enum)
    logging.info('count start: {}'.format(sbc.count_start))
    logging.info('count finish: {}'.format(sbc.count_finish))
    

##
##def calc_rnid_address_gnaf(em_version, debug=False):
##
##    logging.info('environment')
##    em = gis.ESTAMAP(em_version)
##
##    logging.info('creating temp table')
##    arcpy.CreateTable_management(out_path='in_memory',
##                                 out_name='address_gnaf_rnid')
##    arcpy.AddField_management(in_table='in_memory\\address_gnaf_rnid',
##                              field_name='ADDRESS_DETAIL_PID',
##                              field_type='TEXT',
##                              field_length=15)
##    arcpy.AddField_management(in_table='in_memory\\address_gnaf_rnid',
##                              field_name='ROAD_NAME_ID',
##                              field_type='LONG')
##
##    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ADDRESS_GNAF'),
##                               field_names=['ADDRESS_DETAIL_PID', 'STREET_NAME', 'STREET_TYPE_CODE', 'STREET_SUFFIX_CODE'],
##                               sql_clause=(None, 'ORDER BY ADDRESS_DETAIL_PID')) as sc, \
##         arcpy.da.InsertCursor(in_table='in_memory\\address_gnaf_rnid',
##                               field_names=['ADDRESS_DETAIL_PID', 'ROAD_NAME_ID']) as ic:
##
##        route_flag = 0
##        for enum, (pfi, road_name, road_type, road_suffix) in enumerate(sc, 1):
##            
##            road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag = em.parse_roadname(road_name, road_type, road_suffix, route_flag=route_flag)        
##            exists = em.check_roadname(road_name=road_name_parsed, road_type=road_type_parsed, road_suffix=road_suffix_parsed)
##
##            if exists:
##                rnid, cdts, sdx, rf = exists
##                ic.insertRow((pfi, rnid))
##            else:
##                ic.insertRow((pfi, -1))
##
##            if enum % 100000 == 0:
##                logging.info(str(enum))
##        logging.info(str(enum))
##        
##    logging.info('checking output table')
##    address_gnaf_rnids_table = os.path.join(em.sde, 'ADDRESS_GNAF_RNIDS')
##    if arcpy.Exists(address_gnaf_rnids_table):
##        arcpy.Delete_management(address_gnaf_rnids_table)
##
##    logging.info('output to table')
##    arcpy.TableToTable_conversion(in_rows='in_memory\\address_gnaf_rnid',
##                                  out_path=os.path.split(address_gnaf_rnids_table)[0],
##                                  out_name=os.path.split(address_gnaf_rnids_table)[-1])
##    logging.info(str(arcpy.GetCount_management('in_memory\\address_gnaf_rnid').getOutput(0)))
##
##    arcpy.AddIndex_management(in_table=address_gnaf_rnids_table,
##                              fields='ADDRESS_DETAIL_PID',
##                              index_name='IDX_ADD_GNAF_RNIDS_ADDRESS_DETAIL_PID')
##
##    logging.info('update ADDRESS_GNAF_DETAILS')
##    conn = arcpy.ArcSDESQLExecute(em.sde)
##    results = conn.execute('''
##    UPDATE d
##    SET d.ROAD_NAME_ID = r.ROAD_NAME_ID
##    FROM ADDRESS_GNAF_DETAILS d
##    LEFT JOIN ADDRESS_GNAF_RNIDS r
##    ON d.ADDRESS_DETAIL_PID = r.ADDRESS_DETAIL_PID
##    ''')
##    logging.info(str(results))


def register_new_addressgnaf_roadname(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    cursor = em.conn.cursor()

    results = cursor.execute('select count(*) from ROAD_NAME_REGISTER').fetchval()
    logging.info('total ROAD_NAME_REGISTER: {}'.format(results))

    sql_stmt = '''
    select distinct STREET_NAME, STREET_TYPE_CODE, STREET_SUFFIX_CODE from ADDRESS_GNAF
    '''
    
    new_roadnames = []
    route_flag = 0
    for enum, (road_name, road_type, road_suffix) in enumerate(cursor.execute(sql_stmt), 1):

        # parse roadname
        road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag = em.parse_roadname(road_name, road_type, road_suffix, route_flag=route_flag)

        if not em.check_roadname(road_name=road_name_parsed, road_type=road_type_parsed, road_suffix=road_suffix_parsed):
            new_roadnames.append((road_name_parsed, road_type_parsed, road_suffix_parsed))
        em.create_roadname(road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag)

    logging.info('Num New Road Names: {}'.format(len(new_roadnames)))        
    
    results = cursor.execute('select count(*) from ROAD_NAME_REGISTER').fetchval()
    logging.info('total ROAD_NAME_REGISTER: {}'.format(results)) 


def create_address_gnaf_rnid_table(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    sql_script = os.path.join(em.path, 'sql', 'transport', 'create_address_gnaf_rnid.sql')
    logging.info('running sql script: {}'.format(sql_script))

    dbpy.exec_script(em.server, em.database_name, sql_script)


def calc_address_gnaf_rnid(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    cursor = em.conn.cursor()

    sql_stmt = '''
    select ADDRESS_DETAIL_PID, STREET_NAME, STREET_TYPE_CODE, STREET_SUFFIX_CODE
    from ADDRESS_GNAF
    '''
    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'dbo.ADDRESS_GNAF_RNID') as sbc:

        logging.info('looping address_gnaf')
        route_flag = 0
        for enum, (pid, road_name, road_type, road_suffix) in enumerate(cursor.execute(sql_stmt)):

            road_name_parsed, road_type_parsed, road_suffix_parsed, route_flag = em.parse_roadname(road_name, road_type, road_suffix, route_flag)

            # check parsed roadname
            roadname = em.check_roadname(road_name_parsed, road_type_parsed, road_suffix_parsed)
            
            sbc.add_row((pid, roadname.ROAD_NAME_ID))
            
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
        
        estamap_version = args['--estamap_version']
        log_file = args['--log_file']
        log_path = args['--log_path']

        with log.LogFile(log_file, log_path):
            logging.info('start')
            try:
##                register_new_road_roadname(estamap_version)
##                register_new_road_routeno(estamap_version)
##                register_new_road_structurename(estamap_version)
##                register_new_roadinfrastructure_name(estamap_version)
##
##                create_road_alias_table(estamap_version)
##                calc_road_alias(estamap_version)
##
##                create_road_infrastructure_rnid_table(estamap_version)
##                calc_road_infrastructure_rnid(estamap_version)
##
##                # address
                register_new_address_roadname(estamap_version)
##                create_address_rnid_table(estamap_version)
##                calc_address_rnid(estamap_version)
##
##                # address gnaf
##                register_new_addressgnaf_roadname(estamap_version)
##                create_address_gnaf_rnid_table(estamap_version)
##                calc_address_gnaf_rnid(estamap_version)

            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')
