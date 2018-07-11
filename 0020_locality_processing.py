'''
Calculates LOCALITY attributes.

Usage:
  calc_locality_attributes.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  --log_file <file>       Log File name. [default: calc_locality_attributes.log]
  --log_path <folder>     Folder to store the log file. [default: c:\\temp]
'''
import os
import logging

from docopt import docopt
import rtree
import arcpy

import shapely.strtree
import shapely.geometry
import shapely.wkb
import shapely.wkt
import shapely.ops
import shapely.prepared

import log
import dev as gis
import dbpy


def create_locality_centroid(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(3111)
    ingr_sr = gis.ingr_spatial_reference()
    ingr_uor_sr = gis.ingr_uor_spatial_reference()
    
    if arcpy.Exists(os.path.join(em.sde, 'LOCALITY_CENTROID')):
        logging.info('deleting existing locality centroid fc')
        arcpy.Delete_management(os.path.join(em.sde, 'LOCALITY_CENTROID'))

    logging.info('creating locality centroid fc')
    arcpy.CreateFeatureclass_management(out_path='in_memory',
                                        out_name='locality_centroid_temp',
                                        geometry_type='POINT')
    arcpy.AddField_management(in_table='in_memory\\locality_centroid_temp',
                              field_name='PFI',
                              field_type='LONG')
    # vicgrid coords
    arcpy.AddField_management(in_table='in_memory\\locality_centroid_temp',
                              field_name='X_VICGRID',
                              field_type='DOUBLE',
                              field_precision=12,
                              field_scale=3)
    arcpy.AddField_management(in_table='in_memory\\locality_centroid_temp',
                              field_name='Y_VICGRID',
                              field_type='DOUBLE',
                              field_precision=12,
                              field_scale=3)

    # ingr coords
    arcpy.AddField_management(in_table='in_memory\\locality_centroid_temp',
                              field_name='X_INGR',
                              field_type='DOUBLE',
                              field_precision=12,
                              field_scale=3)
    arcpy.AddField_management(in_table='in_memory\\locality_centroid_temp',
                              field_name='Y_INGR',
                              field_type='DOUBLE',
                              field_precision=12,
                              field_scale=3)

    # ingr uor coords
    arcpy.AddField_management(in_table='in_memory\\locality_centroid_temp',
                              field_name='X_INGR_UOR',
                              field_type='DOUBLE',
                              field_precision=12,
                              field_scale=3)
    arcpy.AddField_management(in_table='in_memory\\locality_centroid_temp',
                              field_name='Y_INGR_UOR',
                              field_type='DOUBLE',
                              field_precision=12,
                              field_scale=3)

    arcpy.FeatureToPoint_management(in_features=os.path.join(em.sde, 'LOCALITY'),
                                    out_feature_class='in_memory\\locality_centroid')

    logging.info('calc coordinates...')
    with arcpy.da.SearchCursor(in_table='in_memory\\locality_centroid',
                               field_names=['PFI', 'SHAPE@']) as sc, \
         arcpy.da.InsertCursor(in_table='in_memory\\locality_centroid_temp',
                               field_names=['PFI', 'SHAPE@', 
                                            'X_VICGRID', 'Y_VICGRID',
                                            'X_INGR', 'Y_INGR',
                                            'X_INGR_UOR', 'Y_INGR_UOR',
                                            ]) as ic:

        for enum, (pfi, geom) in enumerate(sc):
            if enum % 100 == 0:
                logging.info(enum)
            geom_ingr = geom.projectAs(ingr_sr)
            geom_ingr_uor = geom.projectAs(ingr_uor_sr)

            ic.insertRow((pfi, geom,
                          geom.centroid.X, geom.centroid.Y,
                          geom_ingr.centroid.X, geom_ingr.centroid.Y,
                          geom_ingr_uor.centroid.X, geom_ingr_uor.centroid.Y))
        logging.info(enum)
        
    logging.info('exporting...')
    arcpy.FeatureClassToFeatureClass_conversion(in_features='in_memory\\locality_centroid_temp',
                                                out_path=em.sde,
                                                out_name='LOCALITY_CENTROID')


def create_locality_detail_table(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    sql_script = os.path.join(em.path, 'sql', 'detail_tables', 'create_locality_detail.sql')
    logging.info('running sql script: {}'.format(sql_script))

    dbpy.exec_script(em.server, em.database_name, sql_script)


def calc_locality_detail(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'LOCALITY'),
                               field_names=['PFI', 'NAME', 'SHAPE@', 'SHAPE@AREA', 'SHAPE@LENGTH']) as sc, \
         arcpy.da.InsertCursor(in_table=os.path.join(em.sde, 'LOCALITY_DETAIL'),
                               field_names=['PFI', 'RING_COUNT', 'SEGMENT_COUNT', 'AREA_SIZE', 'PERIMETER_SIZE', 'SOUNDEX']) as ic:
        for row in sc:
            pfi, name, geom, area_size, perimeter_size = row
            
            ring_count = geom.boundary().partCount
            segment_count = geom.boundary().pointCount - ring_count
            soundex = gis.generate_soundex(name)
            
            ic.insertRow((pfi, ring_count, segment_count, area_size, perimeter_size, soundex))


def calc_locality_nodeid(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(3111)
    cursor = em.conn.cursor()

    logging.info('creating in_memory feature class')
    if arcpy.Exists(os.path.join(em.sde, 'LOCALITY_NODEID')):
        arcpy.Delete_management(os.path.join(em.sde, 'LOCALITY_NODEID'))

    arcpy.CreateFeatureclass_management(out_path='in_memory',
                                        out_name='locality_nodeid_temp',
                                        geometry_type='POINT')
    arcpy.AddField_management(in_table='in_memory\\locality_nodeid_temp',
                              field_name='PFI',
                              field_type='LONG')
    arcpy.AddField_management(in_table='in_memory\\locality_nodeid_temp',
                              field_name='ROAD_INFRA_UFI',
                              field_type='LONG')

    logging.info('loading geoms into memory')
    road_infra_xys = {}
##    for enum, (ufi, x, y) in enumerate(cursor.execute('select ufi, shape.STX, shape.STY from ROAD_INFRASTRUCTURE')):
##        if enum % 100000 == 0:
##            logging.info(enum)
##        road_infra_xys[ufi] = (x, y)
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD_INFRASTRUCTURE'),
                               field_names=['UFI', 'SHAPE@X', 'SHAPE@Y'],
                               sql_clause=(None, 'ORDER BY UFI')) as sc_infra:
        for enum, (ufi, x, y) in enumerate(sc_infra, 1):
            road_infra_xys[ufi] = (x, y)
    logging.info(enum)
    logging.info('num loaded: {}'.format(len(road_infra_xys)))
    

    logging.info('build rtree')
##    def stream_load_infra():
##        with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD_INFRASTRUCTURE'),
##                                   field_names=['UFI', 'SHAPE@X', 'SHAPE@Y'],
##                                   sql_clause=(None, 'ORDER BY UFI')) as sc_infra:
##            for enum, (ufi, x, y) in enumerate(sc_infra, 1):
##                yield (ufi, (x,y,x,y), None)
##                if enum % 100000 == 0:
##                    logging.info(enum)
##            logging.info(enum)

    def stream_load_infra():
        for ufi, (x, y) in road_infra_xys.iteritems():
            yield (ufi, (x, y, x, y), None)
    
    idx_infra = rtree.index.Index(stream_load_infra())
    logging.info('stream end')


    logging.info('looping locality')
    sql = '''
    select
        locality.pfi,
        locality.name,
        locality.shape,
        locality_centroid.shape
    from locality
    left join locality_centroid
    on locality.pfi = locality_centroid.pfi
    order by locality.name
    '''
    
    with arcpy.da.InsertCursor(in_table='in_memory\\locality_nodeid_temp',
                               field_names=['PFI', 'ROAD_INFRA_UFI', 'SHAPE@']) as ic:
        for pfi, name, loc_poly, loc_cent in cursor.execute(sql):
            geom_poly = shapely.prepared.prep(shapely.wkt.loads(loc_poly))
            geom_cent = shapely.wkt.loads(loc_cent)

            road_infra_in_scope = idx_infra.intersection(geom_poly.context.bounds)
            
            road_infra_in_locality = []
            for ufi in road_infra_in_scope:
                road_infra_geom = shapely.geometry.Point(*road_infra_xys[ufi])
                road_infra_geom.ufi = ufi
                if geom_poly.intersects(road_infra_geom):
                    road_infra_in_locality.append(road_infra_geom)

            if len(road_infra_in_locality) > 0:
                _, nearest_point = shapely.ops.nearest_points(geom_cent, shapely.geometry.MultiPoint(road_infra_in_locality))
                ic.insertRow((pfi, road_infra_in_locality[road_infra_in_locality.index(nearest_point)].ufi, arcpy.Point(nearest_point.x, nearest_point.y)))

            if ufi == -1:
                logging.info('no ufi: ' + name)

    logging.info('exporting...')
    arcpy.FeatureClassToFeatureClass_conversion(in_features='in_memory\\locality_nodeid_temp',
                                                out_path=em.sde,
                                                out_name='LOCALITY_NODEID')


def register_new_locality(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    cursor = em.conn.cursor()

    new_localities = []
    for row in cursor.execute('''
    SELECT
        'SDRN' as [VER_FCODE],
        NAME as [VER_TOWN_NAME],
        STATE as [VER_STATE],
        [LOCALITY_DETAIL].[SOUNDEX] as [SOUNDEX],
        [LOCALITY].[PFI] as [SOURCE_PK]
    FROM LOCALITY
    LEFT JOIN LOCALITY_DETAIL
        ON LOCALITY.PFI = LOCALITY_DETAIL.PFI
    WHERE
        LOCALITY.NAME not in (SELECT VER_TOWN_NAME from LOCALITY_MSLINK_REGISTER)
    '''):
        fcode, name, state, sdx, pk, = row
        dataset = 'VICMAP_' + em.vicmap_version
        new_localities.append((fcode, name, state, sdx, dataset, pk))
        logging.info('new locality: {}'.format(name))

    if len(new_localities) > 0:
        for new_locality in new_localities:
            cursor.execute('''
            INSERT INTO [dbo].[LOCALITY_MSLINK_REGISTER]
               ([VER_FCODE]
               ,[VER_TOWN_NAME]
               ,[VER_STATE]
               ,[VER_TOWN_SDX]
               ,[SOURCE_DATASET]
               ,[SOURCE_PK])
            VALUES (?, ?, ?, ?, ?, ?)
            ''', new_locality)
            logging.info('inserting new locality successful: {}'.format(cursor.rowcount))
        cursor.commit()

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
                create_locality_centroid(estamap_version)
                create_locality_detail_table(estamap_version)
                calc_locality_detail(estamap_version)
                calc_locality_nodeid(estamap_version)
                register_new_locality(estamap_version)

            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')
