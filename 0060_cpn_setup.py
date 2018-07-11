'''
setup tables and feature classes for the old cpn process to work with the new process.

Usage:
  cpn_setup.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  
  --log_file <file>       Log File name. [default: cpn_setup.log]
  --log_path <folder>     Folder to store the log file. [default: c:\\temp]
'''
import os
import sys
import time
import logging
import glob

from docopt import docopt
import arcpy

import log
import dev as gis
import dbpy



def import_vicmap_fcs(estamap_version):
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    vm = gis.VICMAP(em.vicmap_version)
    
    fcs = ['LGA_POLYGON',
           'PARCEL_VIEW',
           'PROPERTY_VIEW',
           'TR_ROAD',
           'TR_ROAD_INFRASTRUCTURE',
           'FOI_INDEX_CENTROID',
           'GEO_AREA_HYDRO_LABEL',
           ]
    for fc in fcs:
        if not arcpy.Exists(os.path.join(em.sde, fc)):
            logging.info('importing: {}'.format(fc))
            arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(vm.sde, fc),
                                                        out_path=em.sde,
                                                        out_name=fc)

def import_vicmap_tables(estamap_version):
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    vm = gis.VICMAP(em.vicmap_version)

    tables = ['TR_ROAD_CLASS',
              'PARCEL',
              'PROPERTY',
              'PARCEL_PROPERTY',
              'FT_AUTHORITATIVE_ORGANISATION',
              ]
    for table in tables:
        if not arcpy.Exists(os.path.join(em.sde, table)):
            logging.info('importing: {}'.format(table))
            arcpy.TableToTable_conversion(in_rows=os.path.join(vm.sde, table),
                                          out_path=em.sde,
                                          out_name=table)

               
def add_oid_to_gnaf(estamap_version):
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)
    logging.info('adding objectid')
    conn.execute('''
    IF COL_LENGTH('DBO.ADDRESS_GNAF', 'OBJECTID') IS NULL
    BEGIN
        ALTER TABLE ADDRESS_GNAF ADD OBJECTID [int]
    END
    ''')
    logging.info('updating objectid')
    conn.execute(''' 
        UPDATE X
        SET X.OBJECTID = X.OID
        FROM (
        SELECT ADDRESS_DETAIL_PID, OBJECTID, ROW_NUMBER() OVER(ORDER BY ADDRESS_DETAIL_PID) AS OID
        FROM ADDRESS_GNAF) X;
    ''')
    conn.commit()

def create_address_road_fc(estamap_version, fc_name):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    
    logging.info('creating in_memory {}'.format(fc_name))
    address_road_fc = 'in_memory\\{}'.format(fc_name)
    if arcpy.Exists(address_road_fc):
        arcpy.Delete_management(address_road_fc)
    arcpy.CreateFeatureclass_management(out_path='in_memory',
                                        out_name=fc_name,
                                        geometry_type='POLYLINE',
                                        spatial_reference=arcpy.SpatialReference(3111))
    
    logging.info('adding fields')    
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='CREATED_DATETIME',
                              field_type='DATE')
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='MODIFIED_DATETIME',
                              field_type='DATE')
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='UFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='Length',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='ADDRESS_PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='ROAD_PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='ROAD_X_PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='SIDE_OF_ROAD',
                              field_type='TEXT',
                              field_length=1)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='MATCH_METHOD',
                              field_type='TEXT',
                              field_length=20)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='INPUT_ROAD_NAME_ID',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='MATCH_ROAD_NAME_ID',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='CROSS_COUNT',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='DISTANCE_FROM_FEATURE',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='DISTANCE_ALONG_FEATURE',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='SCORE',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='LOCALITY_NAME',
                              field_type='TEXT',
                              field_length=50)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='SOUNDEX',
                              field_type='TEXT',
                              field_length=5)
    
    logging.info('copying to sde: {}'.format(fc_name))
    if arcpy.Exists(os.path.join(em.sde, fc_name)):
        arcpy.Delete_management(os.path.join(em.sde, fc_name))
    arcpy.FeatureClassToFeatureClass_conversion(in_features=address_road_fc,
                                                out_path=em.sde,
                                                out_name=fc_name)
    
def populate_address_road(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    
    logging.info('creating in_memory ADDRESS_ROAD')
    address_road_fc = 'in_memory\\ADDRESS_ROAD'
    if arcpy.Exists(address_road_fc):
        arcpy.Delete_management(address_road_fc)
    arcpy.CreateFeatureclass_management(out_path='in_memory',
                                        out_name='ADDRESS_ROAD',
                                        geometry_type='POLYLINE',
                                        spatial_reference=arcpy.SpatialReference(3111))
    
    logging.info('adding fields')    
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='CREATED_DATETIME',
                              field_type='DATE')
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='MODIFIED_DATETIME',
                              field_type='DATE')
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='UFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='Length',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='ADDRESS_PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='ROAD_PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='ROAD_X_PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='SIDE_OF_ROAD',
                              field_type='TEXT',
                              field_length=1)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='MATCH_METHOD',
                              field_type='TEXT',
                              field_length=20)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='INPUT_ROAD_NAME_ID',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='MATCH_ROAD_NAME_ID',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='CROSS_COUNT',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='DISTANCE_FROM_FEATURE',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='DISTANCE_ALONG_FEATURE',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='SCORE',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='LOCALITY_NAME',
                              field_type='TEXT',
                              field_length=50)
    arcpy.AddField_management(in_table=address_road_fc,
                              field_name='SOUNDEX',
                              field_type='TEXT',
                              field_length=5)

    logging.info('loading from ADDRESS_ROAD_VALIDATION')
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ADDRESS_ROAD_VALIDATION'),
                               field_names=['ADDR_PFI', 'ROAD_PFI', 'RULE_RANK', 'RULE_CODE', 'RULE_SCORE', 'VALID_ATTR', 'VALID_SPATIAL', 'ADDR_RNID', 'ROAD_RNID', 'ADDR_LOCALITY_NAME', 'ROAD_LOCALITY_NAME', 'ADDR_SOUNDEX', 'ROAD_SOUNDEX', 'INTERSECTS', 'SIDE_OF_ROAD', 'DIST_FROM_ROAD', 'DIST_ALONG_ROAD', 'Shape']) as sc, \
         arcpy.da.InsertCursor(in_table='in_memory\\ADDRESS_ROAD',
                               field_names=['CREATED_DATETIME', 'MODIFIED_DATETIME', 'PFI', 'UFI', 'Length', 'ADDRESS_PFI', 'ROAD_PFI', 'ROAD_X_PFI', 'SIDE_OF_ROAD', 'MATCH_METHOD', 'INPUT_ROAD_NAME_ID', 'MATCH_ROAD_NAME_ID', 'CROSS_COUNT', 'DISTANCE_FROM_FEATURE', 'DISTANCE_ALONG_FEATURE', 'SCORE', 'LOCALITY_NAME', 'SOUNDEX', 'Shape']) as ic:

        for enum, row in enumerate(sc):
            addr_pfi, road_pfi, rule_rank, rule_code, rule_score, valid_attr, valid_spatial, addr_rnid, road_rnid, addr_locality_name, road_locality_name, addr_soundex, road_soundex, intersects, side_of_road, dist_from_road, dist_along_road, geom = row
            new_row = [
                None,
                None,
                None,
                None,
                dist_from_road,
                addr_pfi,
                road_pfi,
                None,
                side_of_road,
                rule_code,
                addr_rnid,
                road_rnid,
                intersects,
                dist_from_road,
                dist_along_road,
                rule_score,
                addr_locality_name,
                addr_soundex,
                geom,
                ]
            ic.insertRow(new_row)

            if enum % 100000 == 0:
                logging.info(enum)
        logging.info(enum)

    logging.info('copying to sde: ADDRESS_ROAD')
    if arcpy.Exists(os.path.join(em.sde, 'ADDRESS_ROAD')):
        arcpy.Delete_management(os.path.join(em.sde, 'ADDRESS_ROAD'))
    arcpy.FeatureClassToFeatureClass_conversion(in_features=address_road_fc,
                                                out_path=em.sde,
                                                out_name='ADDRESS_ROAD')

    
    
def create_tr_road_name(estamap_version):
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)

    if not dbpy.check_exists('TR_ROAD_NAME', conn):
        conn.execute('''
        SELECT * INTO TR_ROAD_NAME
        FROM ROAD_NAME_REGISTER
        ''')


def old_address_scripts(estamap_version):
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    dbpy.exec_script(em.server, em.database_name, os.path.join(em.path, 'SQL' ,'CPN', 'Address', '1000_GENERIC_MAP_Tables.sql'))
    dbpy.exec_script(em.server, em.database_name, os.path.join(em.path, 'SQL' ,'CPN', 'Address', 'AddressValidationRule.sql'))
    
def old_indexing_scripts(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    
    for script in glob.glob(os.path.join(em.path, 'SQL', 'CPN', 'Indexing', '*.sql')):
        logging.info('executing script: {}'.format(script))
        dbpy.exec_script(em.server, em.database_name, script)

def old_transport_scripts(estamap_version):
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    
    for script in glob.glob(os.path.join(em.path, 'SQL', 'CPN', 'Transport', '*.sql')):
        logging.info('executing script: {}'.format(script))
        dbpy.exec_script(em.server, em.database_name, script)

def old_views_scripts(estamap_version):
    pass


def populate_address_details(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)

    logging.info('ADDRESS_DETAILS')
    conn.execute('''
    INSERT INTO ADDRESS_DETAILS(OBJECTID, PFI)
    SELECT CAST(PFI AS INT), CAST(PFI AS INT)
    FROM ADDRESS
    ORDER BY LOCALITY_NAME, ROAD_NAME, ROAD_TYPE, ROAD_SUFFIX, HOUSE_NUMBER_1, BLG_UNIT_ID_1
    GO
    ''')


    logging.info('ADDRESS_GNAF_DETAILS')

    

def sync_address_esta(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)
    
    logging.info('creating view: vwADDRESS_ESTA')
    dbpy.exec_script(em.server, em.database_name, os.path.join(em.path, 'SQL', 'CPN', 'vwADDRESS_ESTA.sql'))

   
    logging.info('creating in_memory ADDRESS_ESTA')
    address_esta_fc = 'in_memory\\ADDRESS_ESTA'
    if arcpy.Exists(address_esta_fc):
        arcpy.Delete_management(address_esta_fc)
    arcpy.CreateFeatureclass_management(out_path='in_memory',
                                        out_name='ADDRESS_ESTA',
                                        geometry_type='POINT',
                                        spatial_reference=arcpy.SpatialReference(3111))
    
    logging.info('adding fields')
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='CREATED_DATETIME',
                              field_type='DATE')
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='MODIFIED_DATETIME',
                              field_type='DATE')
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='UFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='VicGrid94_X',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='VicGrid94_Y',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='INGR94_X',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='INGR94_Y',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='ESZ',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='AMBULANCE',
                              field_type='TEXT',
                              field_length=7)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='FIRE',
                              field_type='TEXT',
                              field_length=7)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='POLICE',
                              field_type='TEXT',
                              field_length=7)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='SES',
                              field_type='TEXT',
                              field_length=7)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='AMBULANCE_PERCENT',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='FIRE_PERCENT',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='POLICE_PERCENT',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='SES_PERCENT',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='VicMapAddressPFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='VicMapAddressUFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='VicMapPropertyPFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='EZI_ADDRESS',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='CAD_STRING',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='CAD_LV_APT',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='CAD_ST_NUM',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='NUMBER_1',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='NUMBER_2',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='ROAD_NAME',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='ROAD_TYPE',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='ROAD_SUFFIX',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='ROAD_NAME_ID',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='LOCALITY_NAME',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='PREMISE_NAME',
                              field_type='TEXT',
                              field_length=80)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='TEMPORARY_ADDRESS',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='MSLINK',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='VicMapRoadPFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='VicMapRoadXPFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=address_esta_fc,
                              field_name='MESSAGE',
                              field_type='TEXT',
                              field_length=8000)
##                arcpy.AddField_management(in_table=address_esta_fc,
##                                          field_name='NODE_ID',
##                                          field_type='LONG',
##                                          field_precision=10)

    field_names = ['CREATED_DATETIME', 'MODIFIED_DATETIME', 'PFI', 'UFI', 'VicGrid94_X', 'VicGrid94_Y', 'INGR94_X', 'INGR94_Y', 'ESZ', 'AMBULANCE', 'FIRE', 'POLICE', 'SES', 'AMBULANCE_PERCENT', 'FIRE_PERCENT', 'POLICE_PERCENT', 'SES_PERCENT', 'VicMapAddressPFI', 'VicMapAddressUFI', 'VicMapPropertyPFI', 'EZI_ADDRESS', 'CAD_STRING', 'CAD_LV_APT', 'CAD_ST_NUM', 'NUMBER_1', 'NUMBER_2', 'ROAD_NAME', 'ROAD_TYPE', 'ROAD_SUFFIX', 'ROAD_NAME_ID', 'LOCALITY_NAME', 'PREMISE_NAME', 'TEMPORARY_ADDRESS', 'MSLINK', 'VicMapRoadPFI', 'VicMapRoadXPFI', 'MESSAGE', 'WKT']
    shape_field_index = [f.upper() for f in field_names].index('WKT')

    insert_field_names = list(field_names)
    insert_field_names[shape_field_index] = 'SHAPE@WKT'
    

    logging.info('opening cursors')
    with arcpy.da.InsertCursor(in_table=address_esta_fc,
                               field_names=insert_field_names) as ic_address_esta, \
         conn.execute('''select {} from vwADDRESS_ESTA'''.format(','.join(field_names))) as rows:
        
        for enum, row in enumerate(rows):
            ic_address_esta.insertRow(row)
            
            if enum % 100000 == 0:
                logging.info(enum)
        logging.info(enum)

    
    logging.info('copying to sde: ADDRESS_ESTA')
    if arcpy.Exists(os.path.join(em.sde, 'ADDRESS_ESTA')):
        arcpy.Delete_management(os.path.join(em.sde, 'ADDRESS_ESTA'))
    arcpy.FeatureClassToFeatureClass_conversion(in_features=address_esta_fc,
                                                out_path=em.sde,
                                                out_name='ADDRESS_ESTA')

    logging.info('feature count: {}'.format(arcpy.GetCount_management(os.path.join(em.sde, 'ADDRESS_ESTA')).getOutput(0)))


def sync_tr_road_alias_static(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)
    
    logging.info('creating view: vwTR_ROAD_ALIAS_STATIC')
    dbpy.exec_script(em.server, em.database_name, os.path.join(em.path, 'SQL', 'CPN', 'vwTR_ROAD_ALIAS_STATIC.sql'))

   
    logging.info('creating in_memory vwTR_ROAD_ALIAS_STATIC')
    road_fc = 'in_memory\\TR_ROAD_ALIAS_STATIC'
    if arcpy.Exists(road_fc):
        arcpy.Delete_management(road_fc)
    arcpy.CreateFeatureclass_management(out_path='in_memory',
                                        out_name='TR_ROAD_ALIAS_STATIC',
                                        geometry_type='POLYLINE',
                                        spatial_reference=arcpy.SpatialReference(3111))

    logging.info('adding fields')
    arcpy.AddField_management(in_table=road_fc,
                              field_name='PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='UFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='CLASS_CODE',
                              field_type='SHORT',
                              field_precision=5)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='FEATURE_TYPE_CODE',
                              field_type='TEXT',
                              field_length=30)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='FROM_UFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='TO_UFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='LABEL_STRING',
                              field_type='TEXT',
                              field_length=64)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='ROAD_NAME_ID',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='ROAD_NAME',
                              field_type='TEXT',
                              field_length=45)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='ROAD_TYPE',
                              field_type='TEXT',
                              field_length=15)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='ROAD_SUFFIX',
                              field_type='TEXT',
                              field_length=2)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='SOUNDEX',
                              field_type='TEXT',
                              field_length=5)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='ROUTE_FLAG',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='ALIAS_NUMBER',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='ALIAS_COUNT',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='LEFT_LOCALITY',
                              field_type='TEXT',
                              field_length=40)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='RIGHT_LOCALITY',
                              field_type='TEXT',
                              field_length=40)
    arcpy.AddField_management(in_table=road_fc,
                              field_name='LABEL_FLAG',
                              field_type='LONG',
                              field_precision=10)

    field_names = ['PFI', 'UFI', 'CLASS_CODE', 'FEATURE_TYPE_CODE', 'FROM_UFI', 'TO_UFI', 'LABEL_STRING', 'ROAD_NAME_ID', 'ROAD_NAME', 'ROAD_TYPE', 'ROAD_SUFFIX', 'SOUNDEX', 'ROUTE_FLAG', 'ALIAS_NUMBER', 'ALIAS_COUNT', 'LEFT_LOCALITY', 'RIGHT_LOCALITY', 'LABEL_FLAG', 'WKT']
    shape_field_index = [f.upper() for f in field_names].index('WKT')

    insert_field_names = list(field_names)
    insert_field_names[shape_field_index] = 'SHAPE@WKT'

    logging.info('opening cursors')
    with arcpy.da.InsertCursor(in_table=road_fc,
                               field_names=insert_field_names) as ic_road, \
         conn.execute('''select {} from vwTR_ROAD_ALIAS_STATIC'''.format(','.join(field_names))) as rows:
        
        for enum, row in enumerate(rows):
            ic_road.insertRow(row)
            
            if enum % 100000 == 0:
                logging.info(enum)
        logging.info(enum)
    
    logging.info('copying to sde: TR_ROAD_ALIAS_STATIC')
    if arcpy.Exists(os.path.join(em.sde, 'TR_ROAD_ALIAS_STATIC')):
        arcpy.Delete_management(os.path.join(em.sde, 'TR_ROAD_ALIAS_STATIC'))
    arcpy.FeatureClassToFeatureClass_conversion(in_features=road_fc,
                                                out_path=em.sde,
                                                out_name='TR_ROAD_ALIAS_STATIC')

    logging.info('feature count: {}'.format(arcpy.GetCount_management(os.path.join(em.sde, 'TR_ROAD_ALIAS_STATIC')).getOutput(0)))


def sync_tr_road_infra_alias_static(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)
    
    logging.info('creating view: vwTR_ROAD_INFRA_ALIAS_STATIC')
    dbpy.exec_script(em.server, em.database_name, os.path.join(em.path, 'SQL', 'CPN', 'vwTR_ROAD_INFRA_ALIAS_STATIC.sql'))

   
    logging.info('creating in_memory vwTR_ROAD_INFRA_ALIAS_STATIC')
    road_infra_fc = 'in_memory\\TR_ROAD_INFRA_ALIAS_STATIC'
    if arcpy.Exists(road_infra_fc):
        arcpy.Delete_management(road_infra_fc)
    arcpy.CreateFeatureclass_management(out_path='in_memory',
                                        out_name='TR_ROAD_INFRA_ALIAS_STATIC',
                                        geometry_type='POINT',
                                        spatial_reference=arcpy.SpatialReference(3111))

    logging.info('adding fields')
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='UFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='A_ROAD_NAME_ID',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='A_ROAD_NAME',
                              field_type='TEXT',
                              field_length=40)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='A_ROAD_TYPE',
                              field_type='TEXT',
                              field_length=15)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='A_ROAD_SUFFIX',
                              field_type='TEXT',
                              field_length=2)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='A_SOUNDEX',
                              field_type='TEXT',
                              field_length=5)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='B_ROAD_NAME_ID',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='B_ROAD_NAME',
                              field_type='TEXT',
                              field_length=40)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='B_ROAD_TYPE',
                              field_type='TEXT',
                              field_length=15)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='B_ROAD_SUFFIX',
                              field_type='TEXT',
                              field_length=2)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='B_SOUNDEX',
                              field_type='TEXT',
                              field_length=5)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='MSLINK',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='LOCALITY_NAME',
                              field_type='TEXT',
                              field_length=40)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='MUN_SDX',
                              field_type='TEXT',
                              field_length=4)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='XVALUE',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='YVALUE',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='ESZ',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=road_infra_fc,
                              field_name='VERSION',
                              field_type='TEXT',
                              field_length=10)

    
    field_names = ['PFI', 'UFI', 'A_ROAD_NAME_ID', 'A_ROAD_NAME', 'A_ROAD_TYPE', 'A_ROAD_SUFFIX', 'A_SOUNDEX', 'B_ROAD_NAME_ID', 'B_ROAD_NAME', 'B_ROAD_TYPE', 'B_ROAD_SUFFIX', 'B_SOUNDEX', 'MSLINK', 'LOCALITY_NAME', 'MUN_SDX', 'XVALUE', 'YVALUE', 'ESZ', 'VERSION', 'WKT']
    shape_field_index = [f.upper() for f in field_names].index('WKT')

    insert_field_names = list(field_names)
    insert_field_names[shape_field_index] = 'SHAPE@WKT'

    logging.info('opening cursors')
    with arcpy.da.InsertCursor(in_table=road_infra_fc,
                               field_names=insert_field_names) as ic_road_infra, \
         conn.execute('''select {} from vwTR_ROAD_INFRA_ALIAS_STATIC'''.format(','.join(field_names))) as rows:
        
        for enum, row in enumerate(rows):
            ic_road_infra.insertRow(row)
            
            if enum % 100000 == 0:
                logging.info(enum)
        logging.info(enum)
    
    logging.info('copying to sde: TR_ROAD_INFRA_ALIAS_STATIC')
    if arcpy.Exists(os.path.join(em.sde, 'TR_ROAD_INFRA_ALIAS_STATIC')):
        arcpy.Delete_management(os.path.join(em.sde, 'TR_ROAD_INFRA_ALIAS_STATIC'))
    arcpy.FeatureClassToFeatureClass_conversion(in_features=road_infra_fc,
                                                out_path=em.sde,
                                                out_name='TR_ROAD_INFRA_ALIAS_STATIC')

    logging.info('feature count: {}'.format(arcpy.GetCount_management(os.path.join(em.sde, 'TR_ROAD_INFRA_ALIAS_STATIC')).getOutput(0)))


def setup_ad_locality_area_polygon_vg(estamap_version):

    # M:\estamap\18\DatabaseScripts\Views\LocalityViews.sql
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)

    logging.info('creating AD_LOCALITY_AREA_POLYGON_VG')
    if arcpy.Exists(os.path.join(em.sde, 'AD_LOCALITY_AREA_POLYGON_VG')):
        arcpy.Delete_management(os.path.join(em.sde, 'AD_LOCALITY_AREA_POLYGON_VG'))
    arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(em.sde, 'LOCALITY'),
                                                out_path=em.sde,
                                                out_name='AD_LOCALITY_AREA_POLYGON_VG')

    conn.execute('''
    UPDATE L
    SET L.NODE_ID = ISNULL(LN.ROAD_INFRA_UFI, -1)
    FROM AD_LOCALITY_AREA_POLYGON_VG L
    LEFT JOIN LOCALITY_NODEID LN
    ON L.PFI = LN.PFI
    ''')
    conn.execute('''
    UPDATE L
    SET
        L.X_CORD = LC.X_INGR_UOR,
        L.Y_CORD = LC.Y_INGR_UOR
    FROM AD_LOCALITY_AREA_POLYGON_VG L
    LEFT JOIN LOCALITY_CENTROID LC
    ON L.PFI = LC.PFI
    ''')
    conn.execute('''
    UPDATE L
    SET
        L.RING_COUNT = LD.RING_COUNT,
        L.SEGMENT_COUNT = LD.SEGMENT_COUNT,
        L.AREA_SIZE = LD.AREA_SIZE,
        L.PERIMETER_SIZE = LD.PERIMETER_SIZE,
        L.SOUNDEX = LD.SOUNDEX
    FROM AD_LOCALITY_AREA_POLYGON_VG L
    LEFT JOIN LOCALITY_DETAIL LD
    ON L.PFI = LD.PFI
    ''')
    conn.execute('''
    UPDATE L
    SET
        L.ESZ = LMR.MSLINK
    FROM AD_LOCALITY_AREA_POLYGON_VG L
    LEFT JOIN LOCALITY_MSLINK_REGISTER LMR
    ON L.NAME = LMR.VER_TOWN_NAME
    ''')
    conn.commit()
    
    
##    # copy 18 VER_MUN_ALIAS_ESTA just once
##    if not dbpy.check_exists('VER_MUN_ALIAS_ESTA', conn):
##        logging.info('copying EM18 VER_MUN_ALIAS_ESTA')
##        conn.execute('''
##        SELECT * INTO VER_MUN_ALIAS_ESTA
##        FROM ESTAMAP_18_SDE.DBO.VER_MUN_ALIAS_ESTA
##        ''')
##        conn.commit()
##        count = conn.execute('select count(*) from VER_MUN_ALIAS_ESTA').fetchval()
##        logging.info('VER_MUN_ALIAS_ESTA: {}'.format(count))
##
##    logging.info('creating view: vLOCALITY_ALIAS')
##    dbpy.exec_script(em.server, em.database_name, os.path.join(em.path, 'SQL', 'CPN', 'vLOCALITY_ALIAS.sql'))


def setup_ad_lga_area_polygon_vg(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)

    logging.info('creating AD_LGA_AREA_POLYGON_VG')
    if arcpy.Exists(os.path.join(em.sde, 'AD_LGA_AREA_POLYGON_VG')):
        arcpy.Delete_management(os.path.join(em.sde, 'AD_LGA_AREA_POLYGON_VG'))
    arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(em.sde, 'LGA'),
                                                out_path=em.sde,
                                                out_name='AD_LGA_AREA_POLYGON_VG')


##def setup_waterbody(estamap_version):
##
##    logging.info('environment')
##    em = gis.ESTAMAP(estamap_version)
##    vm = gis.VICMAP(em.vicmap_version)
##    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)
##
##    logging.info('creating AD_LGA_AREA_POLYGON_VG')
##    if arcpy.Exists(os.path.join(em.sde, 'GEO_AREA_HYDRO_LABEL')):
##        arcpy.Delete_management(os.path.join(em.sde, 'GEO_AREA_HYDRO_LABEL'))
##    arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(vm.sde, 'GEO_AREA_HYDRO_LABEL'),
##                                                out_path=em.sde,
##                                                out_name='GEO_AREA_HYDRO_LABEL')

##def setup_property(estamap_version):
##
##    logging.info('environment')
##    em = gis.ESTAMAP(estamap_version)
##    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)
##
##    logging.info('creating view: vPROPERTY_ADDRESS')
##    dbpy.exec_script(em.server, em.database_name, os.path.join(em.path, 'SQL', 'CPN', 'vPROPERTY_ADDRESS.sql'))


def setup_poi_fcs(estamap_version, suffix=''):
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    fc_name = 'POINT_OF_INTEREST'
    qa_fc_name = 'POI_QA'
    if suffix:
        fc_name = fc_name + '_' + suffix
        qa_fc_name = qa_fc_name + '_' + suffix

    logging.info('creating {}'.format(fc_name))
    if arcpy.Exists(os.path.join(em.sde, fc_name)):
        arcpy.Delete_management(os.path.join(em.sde, fc_name))
    arcpy.CreateFeatureclass_management(out_path=em.sde,
                                        out_name=fc_name,
                                        geometry_type='POINT',
                                        spatial_reference=arcpy.SpatialReference(3111))
    
    poi_fc = os.path.join(em.sde, fc_name)
    logging.info('adding fields')
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='CREATED_DATETIME',
                              field_type='DATE')
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='MODIFIED_DATETIME',
                              field_type='DATE')
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='UFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='VicGrid94_X',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='VicGrid94_Y',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='INGR94_X',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='INGR94_Y',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='ESZ',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='AMBULANCE',
                              field_type='TEXT',
                              field_length=7)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='FIRE',
                              field_type='TEXT',
                              field_length=7)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='POLICE',
                              field_type='TEXT',
                              field_length=7)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='SES',
                              field_type='TEXT',
                              field_length=7)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='AMBULANCE_PERCENT',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='FIRE_PERCENT',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='POLICE_PERCENT',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='SES_PERCENT',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='VicMapAddressPFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='VicMapAddressUFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='VicMapPropertyPFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='EZI_ADDRESS',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='CAD_STRING',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='CAD_LV_APT',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='CAD_ST_NUM',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='NUMBER_1',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='NUMBER_2',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='ROAD_NAME',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='ROAD_TYPE',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='ROAD_SUFFIX',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='ROAD_NAME_ID',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='LOCALITY_NAME',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='PREMISE_NAME',
                              field_type='TEXT',
                              field_length=80)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='TEMPORARY_ADDRESS',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='MSLINK',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='VicMapRoadPFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='VicMapRoadXPFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='MESSAGE',
                              field_type='TEXT',
                              field_length=8000)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='NODE_ID',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='CAT',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='TYPE',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='NAME',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='SOURCE_DATA_SET',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='SOURCE_SEQUENCE',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='SOURCE_PK',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='COM_NME',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='VERIFIED_DATE',
                              field_type='DATE')
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='VERIFIED_BY',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='VER_CLASS',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='ALIAS_NUMBER',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='SOUNDEX',
                              field_type='TEXT',
                              field_length=5)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='BES_UFI',
                              field_type='TEXT',
                              field_length=11)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='SKM_PFI_22',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='PLAN_ZONE_CODE',
                              field_type='TEXT',
                              field_length=7)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='QUALITY_SCORE',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='DUPLICATE',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='DUPLICATE_PASS',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='OFFICIAL_FLAG',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='LOCALITY_SUFFIX',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='COPL_MSLINK',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='SPAD_MSLINK',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='ALARM',
                              field_type='TEXT',
                              field_length=80)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='COPL_STATUS',
                              field_type='TEXT',
                              field_length=80)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='SPAD_STATUS',
                              field_type='TEXT',
                              field_length=80)
    arcpy.AddField_management(in_table=poi_fc,
                              field_name='LGA',
                              field_type='TEXT',
                              field_length=50)

    
    logging.info('creating {}'.format(qa_fc_name))
    if arcpy.Exists(os.path.join(em.sde, qa_fc_name)):
        arcpy.Delete_management(os.path.join(em.sde, qa_fc_name))
    arcpy.CreateFeatureclass_management(out_path=em.sde,
                                        out_name=qa_fc_name,
                                        geometry_type='POLYLINE',
                                        spatial_reference=arcpy.SpatialReference(3111))
    
    poi_qa_fc = os.path.join(em.sde, qa_fc_name)
    logging.info('adding fields')
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='CREATED_DATETIME',
                              field_type='DATE')
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='MODIFIED_DATETIME',
                              field_type='DATE')
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='UFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='Length',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='ADDRESS_PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='ROAD_PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='ROAD_X_PFI',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='INPUT_ROAD_NAME_ID',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='SIDE_OF_ROAD',
                              field_type='TEXT',
                              field_length=1)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='MATCH_METHOD',
                              field_type='TEXT',
                              field_length=20)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='MATCH_DATASET',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='MATCH_ROAD_NAME_ID',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='CROSS_COUNT',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='DISTANCE_FROM_FEATURE',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='DISTANCE_ALONG_FEATURE',
                              field_type='DOUBLE',
                              field_precision=38,
                              field_scale=8)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='SCORE',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='LOCALITY_NAME',
                              field_type='TEXT',
                              field_length=50)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='SOUNDEX',
                              field_type='TEXT',
                              field_length=5)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='POI_ID',
                              field_type='LONG',
                              field_precision=10)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='SOURCE_DATA_SET',
                              field_type='TEXT',
                              field_length=255)
    arcpy.AddField_management(in_table=poi_qa_fc,
                              field_name='SOURCE_PK',
                              field_type='TEXT',
                              field_length=255)

def populate_poi_tables(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)

    logging.info('populate poi tables')
    conn.execute('set identity_insert poi_source_data_address on;')
    conn.execute('insert into poi_source_data_address ([SOURCEID],[MSLINK_OFFSET],[USE_DATA],[SOURCE_CONNECTION],[SOURCE_FEATURE_CLASS],[SOURCE_CONNECTION_TYPE],[SOURCE_FILTER],[VER_CLASS],[CAT],[TYPE],[ALIAS_TYPE],[CPN_RULE],[CPN_ALIAS_RULE],[CPN_ALIAS_RULE2],[CREATE_NMONOMIC],[MATCH_ADDRESS],[MATCH_ADDRESS_PROPERTY],[MATCH_ADDRESS_RULE],[MATCH_INTERSECTION],[MATCH_INTERSECTION_RULE],[MATCH_ROAD_RULE],[PRIMARY_KEY_FIELD],[ADDRESS_PFI_FIELD],[ADDRESS_NUMBER_FIELD],[ADDRESS_APPARTMENT_FIELD],[ROAD_NAME_FIELD],[ROAD_TYPE_FIELD],[ROAD_SUFFIX_FIELD],[ROAD_NAME_B_FIELD],[ROAD_TYPE_B_FIELD],[ROAD_SUFFIX_B_FIELD],[UNPARSED_ADDRESS_FIELD1],[UNPARSED_ADDRESS_FIELD2],[LOCALITY_FIELD],[STATE_FIELD],[POSTCODE_FIELD],[PROJECTION],[X_FIELD],[Y_FIELD],[MESSAGE_FIELD],[VMAS_WORKFLOW]) select [SOURCEID],[MSLINK_OFFSET],[USE_DATA],[SOURCE_CONNECTION],[SOURCE_FEATURE_CLASS],[SOURCE_CONNECTION_TYPE],[SOURCE_FILTER],[VER_CLASS],[CAT],[TYPE],[ALIAS_TYPE],[CPN_RULE],[CPN_ALIAS_RULE],[CPN_ALIAS_RULE2],[CREATE_NMONOMIC],[MATCH_ADDRESS],[MATCH_ADDRESS_PROPERTY],[MATCH_ADDRESS_RULE],[MATCH_INTERSECTION],[MATCH_INTERSECTION_RULE],[MATCH_ROAD_RULE],[PRIMARY_KEY_FIELD],[ADDRESS_PFI_FIELD],[ADDRESS_NUMBER_FIELD],[ADDRESS_APPARTMENT_FIELD],[ROAD_NAME_FIELD],[ROAD_TYPE_FIELD],[ROAD_SUFFIX_FIELD],[ROAD_NAME_B_FIELD],[ROAD_TYPE_B_FIELD],[ROAD_SUFFIX_B_FIELD],[UNPARSED_ADDRESS_FIELD1],[UNPARSED_ADDRESS_FIELD2],[LOCALITY_FIELD],[STATE_FIELD],[POSTCODE_FIELD],[PROJECTION],[X_FIELD],[Y_FIELD],[MESSAGE_FIELD],[VMAS_WORKFLOW] from estamap_18_sde.dbo.poi_source_data_address')
    conn.execute('set identity_insert poi_source_data_address off;')
    conn.execute('set identity_insert poi_source_data_transport on;')
    conn.execute('insert into poi_source_data_transport ([SOURCEID],[MSLINK_OFFSET],[USE_DATA],[SOURCE_CONNECTION],[SOURCE_FEATURE_CLASS],[SOURCE_CONNECTION_TYPE],[SOURCE_FILTER],[VER_CLASS],[CAT],[TYPE],[ALIAS_TYPE],[CPN_RULE],[CPN_ALIAS_RULE],[CPN_ALIAS_RULE2],[CREATE_NMONOMIC],[MATCH_ADDRESS],[MATCH_ADDRESS_PROPERTY],[MATCH_ADDRESS_RULE],[MATCH_INTERSECTION],[MATCH_INTERSECTION_RULE],[MATCH_ROAD_RULE],[PRIMARY_KEY_FIELD],[ADDRESS_PFI_FIELD],[ADDRESS_NUMBER_FIELD],[ADDRESS_APPARTMENT_FIELD],[ROAD_NAME_FIELD],[ROAD_TYPE_FIELD],[ROAD_SUFFIX_FIELD],[ROAD_NAME_B_FIELD],[ROAD_TYPE_B_FIELD],[ROAD_SUFFIX_B_FIELD],[UNPARSED_ADDRESS_FIELD1],[UNPARSED_ADDRESS_FIELD2],[LOCALITY_FIELD],[STATE_FIELD],[POSTCODE_FIELD],[PROJECTION],[X_FIELD],[Y_FIELD],[MESSAGE_FIELD],[VMAS_WORKFLOW]) select [SOURCEID],[MSLINK_OFFSET],[USE_DATA],[SOURCE_CONNECTION],[SOURCE_FEATURE_CLASS],[SOURCE_CONNECTION_TYPE],[SOURCE_FILTER],[VER_CLASS],[CAT],[TYPE],[ALIAS_TYPE],[CPN_RULE],[CPN_ALIAS_RULE],[CPN_ALIAS_RULE2],[CREATE_NMONOMIC],[MATCH_ADDRESS],[MATCH_ADDRESS_PROPERTY],[MATCH_ADDRESS_RULE],[MATCH_INTERSECTION],[MATCH_INTERSECTION_RULE],[MATCH_ROAD_RULE],[PRIMARY_KEY_FIELD],[ADDRESS_PFI_FIELD],[ADDRESS_NUMBER_FIELD],[ADDRESS_APPARTMENT_FIELD],[ROAD_NAME_FIELD],[ROAD_TYPE_FIELD],[ROAD_SUFFIX_FIELD],[ROAD_NAME_B_FIELD],[ROAD_TYPE_B_FIELD],[ROAD_SUFFIX_B_FIELD],[UNPARSED_ADDRESS_FIELD1],[UNPARSED_ADDRESS_FIELD2],[LOCALITY_FIELD],[STATE_FIELD],[POSTCODE_FIELD],[PROJECTION],[X_FIELD],[Y_FIELD],[MESSAGE_FIELD],[VMAS_WORKFLOW] from estamap_18_sde.dbo.poi_source_data_transport')
    conn.execute('set identity_insert poi_source_data_transport off;')
    conn.execute('set identity_insert poi_source_data_sensis on;')
    conn.execute('insert into poi_source_data_sensis ([SOURCEID],[MSLINK_OFFSET],[USE_DATA],[SOURCE_CONNECTION],[SOURCE_FEATURE_CLASS],[SOURCE_CONNECTION_TYPE],[SOURCE_FILTER],[VER_CLASS],[CAT],[TYPE],[ALIAS_TYPE],[CPN_RULE],[CPN_ALIAS_RULE],[CPN_ALIAS_RULE2],[CREATE_NMONOMIC],[MATCH_ADDRESS],[MATCH_ADDRESS_PROPERTY],[MATCH_ADDRESS_RULE],[MATCH_INTERSECTION],[MATCH_INTERSECTION_RULE],[MATCH_ROAD_RULE],[PRIMARY_KEY_FIELD],[ADDRESS_PFI_FIELD],[ADDRESS_NUMBER_FIELD],[ADDRESS_APPARTMENT_FIELD],[ROAD_NAME_FIELD],[ROAD_TYPE_FIELD],[ROAD_SUFFIX_FIELD],[ROAD_NAME_B_FIELD],[ROAD_TYPE_B_FIELD],[ROAD_SUFFIX_B_FIELD],[UNPARSED_ADDRESS_FIELD1],[UNPARSED_ADDRESS_FIELD2],[LOCALITY_FIELD],[STATE_FIELD],[POSTCODE_FIELD],[PROJECTION],[X_FIELD],[Y_FIELD],[MESSAGE_FIELD],[VMAS_WORKFLOW]) select [SOURCEID],[MSLINK_OFFSET],[USE_DATA],[SOURCE_CONNECTION],[SOURCE_FEATURE_CLASS],[SOURCE_CONNECTION_TYPE],[SOURCE_FILTER],[VER_CLASS],[CAT],[TYPE],[ALIAS_TYPE],[CPN_RULE],[CPN_ALIAS_RULE],[CPN_ALIAS_RULE2],[CREATE_NMONOMIC],[MATCH_ADDRESS],[MATCH_ADDRESS_PROPERTY],[MATCH_ADDRESS_RULE],[MATCH_INTERSECTION],[MATCH_INTERSECTION_RULE],[MATCH_ROAD_RULE],[PRIMARY_KEY_FIELD],[ADDRESS_PFI_FIELD],[ADDRESS_NUMBER_FIELD],[ADDRESS_APPARTMENT_FIELD],[ROAD_NAME_FIELD],[ROAD_TYPE_FIELD],[ROAD_SUFFIX_FIELD],[ROAD_NAME_B_FIELD],[ROAD_TYPE_B_FIELD],[ROAD_SUFFIX_B_FIELD],[UNPARSED_ADDRESS_FIELD1],[UNPARSED_ADDRESS_FIELD2],[LOCALITY_FIELD],[STATE_FIELD],[POSTCODE_FIELD],[PROJECTION],[X_FIELD],[Y_FIELD],[MESSAGE_FIELD],[VMAS_WORKFLOW] from estamap_18_sde.dbo.poi_source_data_sensis')
    conn.execute('set identity_insert poi_source_data_sensis off;')
    conn.execute('insert into poi_category ([VER_CODE],[VER_NAME],[VER_DESC],[SOURCE_DATA_SET],[SOURCE_CATEGORY],[VM_FOI_FEATURE_TYPE],[VM_FOI_FEATURE_SUBTYPE],[PROCESS],[SOUNDEX]) select [VER_CODE],[VER_NAME],[VER_DESC],[SOURCE_DATA_SET],[SOURCE_CATEGORY],[VM_FOI_FEATURE_TYPE],[VM_FOI_FEATURE_SUBTYPE],[PROCESS],[SOUNDEX] from estamap_18_sde.dbo.poi_category')
    conn.execute('insert into MDN_CPN ([MDN_CAD_COM_NME],[MDN],[MDN_TYPE]) select [MDN_CAD_COM_NME],[MDN],[MDN_TYPE] from estamap_18_sde.dbo.MDN_CPN')
    conn.execute('insert into AV_HEWS ([COL_ROW],[HOSP_NAME],[SPAD_MSLINK],[ST_NUM],[FEANME],[FEATYP],[DIRSUF],[MUN],[COM_NME],[ESZ],[MSG],[MSLINK],[TEXT_ID],[X_CORD],[Y_CORD],[FEA_MSLINK],[VICMAP_ADDRESS_PFI],[VICMAP_ROAD_PFI],[CAD_STRING]) select [COL_ROW],[HOSP_NAME],[SPAD_MSLINK],[ST_NUM],[FEANME],[FEATYP],[DIRSUF],[MUN],[COM_NME],[ESZ],[MSG],[MSLINK],[TEXT_ID],[X_CORD],[Y_CORD],[FEA_MSLINK],[VICMAP_ADDRESS_PFI],[VICMAP_ROAD_PFI],[CAD_STRING] from estamap_18_sde.dbo.AV_HEWS')
    conn.execute('insert into AV_PICKLIST ([METRO],[RURAL],[HEWS],[MDN],[CAMPUSCODE],[COL_ROW],[COL_ROW_RURAL],[OPSNAME],[PICKLIST],[MDN_PICKLIST],[MDN_CPN],[CPN_CAD],[NEW_OPSNAME],[NEW_PARAMETER],[ST_NUM],[ROAD_NAME],[ROAD_TYPE],[ROAD_SUFFIX],[LOCALITY]) select [METRO],[RURAL],[HEWS],[MDN],[CAMPUSCODE],[COL_ROW],[COL_ROW_RURAL],[OPSNAME],[PICKLIST],[MDN_PICKLIST],[MDN_CPN],[CPN_CAD],[NEW_OPSNAME],[NEW_PARAMETER],[ST_NUM],[ROAD_NAME],[ROAD_TYPE],[ROAD_SUFFIX],[LOCALITY] from estamap_18_sde.dbo.AV_PICKLIST')
    conn.execute('insert into CPN_ROLLOVER_LIST ([VER_CLASS],[PROCESS],[RECORD_COUNT],[OFFICAL_COUNT]) select [VER_CLASS],[PROCESS],[RECORD_COUNT],[OFFICAL_COUNT] from estamap_18_sde.dbo.CPN_ROLLOVER_LIST')
    conn.execute('insert into POI_DUPLICATE_CODE ([DUPLICATE_CODE],[Description],[RECORD_COUNT],[SOURCE_DATA],[SOURCE_DATA2]) select [DUPLICATE_CODE],[Description],[RECORD_COUNT],[SOURCE_DATA],[SOURCE_DATA2] from estamap_18_sde.dbo.POI_DUPLICATE_CODE')

    tables = ['poi_source_data_address',
              'poi_source_data_transport',
              'poi_source_data_sensis',
              'poi_category',
              'MDN_CPN',
              'AV_HEWS',
              'AV_PICKLIST',
              'CPN_ROLLOVER_LIST',
              'POI_DUPLICATE_CODE',                          
              ]                
    for t in tables:
        count = conn.execute('select count(*) from {}'.format(t)).fetchval()
        logging.info('{}: {}'.format(t, count))

##def setup_cpn_repatch(estamap_version):
##    
##    logging.info('environment')
##    em = gis.ESTAMAP(estamap_version)
##    
##    logging.info('executing poi script: 040_Incident_Repatch_Tables.sql')
##    dbpy.exec_script(em.server, em.database_name, os.path.join(em.path, 'SQL', 'CPN', '040_Incident_Repatch_Tables.sql'))


def populate_cpn_repatch_tables(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)

    logging.info('populate cpn repatch tables')
    conn.execute('insert into CPN_REPATCH ([MSLINK],[COM_NME],[COM_NME_PATCHED],[SPAD_MSLINK],[SPAD_MSLINK_PATCHED],[VER_CLASS],[SOURCE_DATA_SET],[ITSM_NO],[SCRIPT_NAME],[SCRIPT_DETAILS],[status]) select [MSLINK],[COM_NME],[COM_NME_PATCHED],[SPAD_MSLINK],[SPAD_MSLINK_PATCHED],[VER_CLASS],[SOURCE_DATA_SET],[ITSM_NO],[SCRIPT_NAME],[SCRIPT_DETAILS],[status] from estamap_18_sde.dbo.CPN_REPATCH')
    conn.execute('insert into ADDRESS_REPATCH ([MSLINK],[COM_NME],[COM_NME_PATCHED],[ESZ],[ESZ_PATCHED],[FEA_MSLINK],[FEA_MSLINK_PATCHED],[NODE_ID],[NODE_ID_PATCHED],[X_CORD],[X_CORD_PATCHED],[Y_CORD],[Y_CORD_PATCHED],[SII_PFI],[SII_PFI_PATCHED],[TEXT_ID],[TEXT_ID_PATCHED],[ST_NUM],[ST_NUM_PATCHED],[ITSM_NO],[SCRIPT_NAME],[SCRIPT_DETAILS],[status]) select [MSLINK],[COM_NME],[COM_NME_PATCHED],[ESZ],[ESZ_PATCHED],[FEA_MSLINK],[FEA_MSLINK_PATCHED],[NODE_ID],[NODE_ID_PATCHED],[X_CORD],[X_CORD_PATCHED],[Y_CORD],[Y_CORD_PATCHED],[SII_PFI],[SII_PFI_PATCHED],[TEXT_ID],[TEXT_ID_PATCHED],[ST_NUM],[ST_NUM_PATCHED],[ITSM_NO],[SCRIPT_NAME],[SCRIPT_DETAILS],[status] from estamap_18_sde.dbo.ADDRESS_REPATCH')
    conn.execute('insert into TRANSPORT_REPATCH ([MSLINK],[ADDTYP],[ADDTYP_PATCHED],[FRADDL],[FRADDL_PATCHED],[FRADDR],[FRADDR_PATCHED],[TOADDL],[TOADDL_PATCHED],[TOADDR],[TOADDR_PATCHED],[ITSM_NO],[SCRIPT_NAME],[SCRIPT_DETAILS]) select [MSLINK],[ADDTYP],[ADDTYP_PATCHED],[FRADDL],[FRADDL_PATCHED],[FRADDR],[FRADDR_PATCHED],[TOADDL],[TOADDL_PATCHED],[TOADDR],[TOADDR_PATCHED],[ITSM_NO],[SCRIPT_NAME],[SCRIPT_DETAILS] from estamap_18_Sde.dbo.TRANSPORT_REPATCH')

    tables = ['CPN_REPATCH',
              'ADDRESS_REPATCH',
              'TRANSPORT_REPATCH',                      
              ]                
    for t in tables:
        count = conn.execute('select count(*) from {}'.format(t)).fetchval()
        logging.info('{}: {}'.format(t, count))


def import_foi(estamap_version, vicmap_version):

    pass

def copy_tr_road_patch(estamap_version):
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    em18 = gis.ESTAMAP(18)

    if not arcpy.Exists(os.path.join(em.sde, 'TR_ROAD_PATCH'):
##        arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(em.sde, 'ROAD_PATCH'),
##                                                    out_path=em.sde,
##                                                    out_name='TR_ROAD_PATCH')
        arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(em18.sde, 'TR_ROAD_PATCH'),
                                                    out_path=em.sde,
                                                    out_name='TR_ROAD_PATCH')

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

##                import_vicmap_fcs(estamap_version)
##                import_vicmap_tables(estamap_version)
##                setup_ad_locality_area_polygon_vg(estamap_version) # ~20sec
##                setup_ad_lga_area_polygon_vg(estamap_version)  #~12sec
                        
##                add_oid_to_gnaf(estamap_version)
##
##                create_address_road_fc(estamap_version, 'ADDRESS_ROAD')
##                create_address_road_fc(estamap_version, 'ADDRESS_ROAD_GNAF')
##                setup_poi_fcs(estamap_version)
##                setup_poi_fcs(estamap_version, 'TRANSPORT')
##                setup_poi_fcs(estamap_version, 'MARKER')
##                setup_poi_fcs(estamap_version, 'STUN')
##                setup_poi_fcs(estamap_version, 'TEMP')
##                setup_poi_fcs(estamap_version, 'DEECD')
##                setup_poi_fcs(estamap_version, 'DHS')
##                setup_poi_fcs(estamap_version, 'LIQUOR')
##                setup_poi_fcs(estamap_version, 'SENSIS')
##                copy_tr_road_patch(estamap_version)

                
##                old_indexing_scripts(estamap_version)
##                old_address_scripts(estamap_version)
##                old_transport_scripts(estamap_version)
##                old_views_scripts(estamap_version)

                # up to here

                
####                populate_address_details(estamap_version)

####                sync_address_esta(estamap_version)  # ~25min
####                sync_tr_road_alias_static(estamap_version)  # ~12min
####                sync_tr_road_infra_alias_static(estamap_version)  # ~3min
####                populate_address_road(estamap_version)
                

####                setup_waterbody(estamap_version)  #~10sec
####                setup_property(estamap_version)  #~1s

                
####                setup_poi_tables(estamap_version)
####                populate_poi_tables(estamap_version)
                
####                populate_cpn_repatch_tables(estamap_version)
                # add conn.commit on full run
                
                ###########

                
                

                #####

##                import_foi(estamap_version, vicmap_version)
##                
##
##                logging.info('environment')
##                em = gis.ESTAMAP(estamap_version)
##                vm = gis.VICMAP(em.vicmap_version)
##
##                logging.info('import FOI_INDEX_CENTROID')
##                arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(vm.sde, 'FOI_INDEX_CENTROID'),
##                                                            out_path=em.sde,
##                                                            out_name='FOI_INDEX_CENTROID')
##                logging.info('import FT_AUTHORITATIVE_ORGANISATION')
##                arcpy.TableToTable_conversion(in_rows=os.path.join(vm.sde, 'FT_AUTHORITATIVE_ORGANISATION'),
##                                              out_path=em.sde,
##                                              out_name='FT_AUTHORITATIVE_ORGANISATION')
##
##                logging.info('foi spatial view')
##                conn.execute('''
##
##                ''')




                
                
                    
                             
                ###########
            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')
