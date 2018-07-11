'''
Calculates Point Attributes for:
 - TR_ROAD_INFRASTRUCTURE
 - ADDRESS
 - ADDRESS_GNAF

Usage:
  calc_point_attributes.py [options]

Options:
  --estamap_version <version>   ESTAMap Version
  --log_file <file>        Log File name. [default: calc_point_attributes.log]
  --log_path <folder>      Folder to store the log file. [default: c:\\temp]

'''
import time
import os
import sys
import logging
import itertools
import cPickle as pickle
import csv

from docopt import docopt
import rtree
import shapely.prepared
import shapely.geometry
import shapely.wkb
import arcpy

import log
import dev as gis
import dbpy


def create_address_detail_table(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    sql_script = os.path.join(em.path, 'sql', 'detail_tables', 'create_address_detail.sql')
    logging.info('running sql script: {}'.format(sql_script))

    dbpy.exec_script(em.server, em.database_name, sql_script)

    
def create_road_infrastructure_detail_table(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    sql_script = os.path.join(em.path, 'sql', 'detail_tables', 'create_road_infrastructure_detail.sql')
    logging.info('running sql script: {}'.format(sql_script))

    dbpy.exec_script(em.server, em.database_name, sql_script)


def calc_address_detail(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    ingr_sr = gis.ingr_spatial_reference()
    ingr_uor_sr = gis.ingr_uor_spatial_reference()


    logging.info('reading locality geoms')
    locality_geoms = {}
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'LOCALITY'),
                               field_names=['PFI', 'NAME', 'SHAPE@WKB']) as sc:
        for pfi, locality_name, wkb in sc:
            locality_geoms[pfi] = (locality_name, shapely.prepared.prep(shapely.wkb.loads(str(wkb))))
    logging.info(len(locality_geoms))

    logging.info('building locality rtree')
    


    def stream_load_locality():
        for pfi, (locality_name, geom) in locality_geoms.iteritems():
            yield (pfi, geom.context.bounds, None)
    locality_rtree = rtree.index.Index(stream_load_locality())
    
            
    logging.info('reading lga geoms')
    lga_geoms = {}
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'LGA'),
                               field_names=['PFI', 'NAME', 'SHAPE@WKB']) as sc:
        for pfi, lga_name, wkb in sc:
            lga_geoms[pfi] = (lga_name, shapely.prepared.prep(shapely.wkb.loads(str(wkb))))
    logging.info(len(lga_geoms))

    logging.info('building lga rtree')
    def stream_load_lga():
        for pfi, (lga_name, geom) in lga_geoms.iteritems():
            yield (pfi, geom.context.bounds, None)
    lga_rtree = rtree.index.Index(stream_load_lga())


    logging.info('looping ADDRESS...')
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ADDRESS'),
                               field_names=['PFI', 'SHAPE@X', 'SHAPE@Y'],
                               sql_clause=(None, 'ORDER BY PFI')) as sc, \
         arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ADDRESS'),
                               field_names=['PFI', 'SHAPE@X', 'SHAPE@Y'],
                               spatial_reference=ingr_sr,
                               sql_clause=(None, 'ORDER BY PFI')) as sc_ingr, \
         dbpy.SQL_BULK_COPY(em.server, em.database_name, 'dbo.ADDRESS_DETAIL') as sbc:

        total_area = shapely.geometry.Point(0,0).buffer(2.5).area

        for enum, (row_vg, row_ingr) in enumerate(itertools.izip(sc, sc_ingr)):

            addr_pfi, x_vicgrid, y_vicgrid = row_vg
            _, x_ingr, y_ingr = row_ingr
            x_ingr_uor = x_ingr * 100.0
            y_ingr_uor = y_ingr * 100.0
            
            geom = shapely.geometry.Point(x_vicgrid, y_vicgrid)

            # locality
            localities = []
            locality_in_scope = locality_rtree.intersection(geom.bounds)
            for pfi in locality_in_scope:
                if locality_geoms[pfi][1].contains(geom):
                    localities.append(pfi)

            locality_name = 'UNKNOWN'
            locality_percent = 0.0
            if len(localities) == 1:
                locality_name = locality_geoms[localities[0]][0]
                locality_percent = 100.0
                
            elif len(localities) > 1:
                logging.info('within 2 localities: {}'.format(addr_pfi))
                localities_ranked = []
                
                # determine largest locality by area if contained within multiple
                for pfi in localities:
                    locality_name, locality_geom = locality_geoms[pfi]
                    geom_buffer = geom.buffer(2.5)
                    area_geom = locality_geom.context.intersection(geom_buffer)
                    locality_percent = area_geom.area / total_area
                    localities_ranked.append((pfi, locality_name, locality_geom, locality_percent))
                
                locality_name, locality_percent = max(localities_ranked, key=lambda x: x[-1])


            # lga
            lgas = []
            lga_in_scope = lga_rtree.intersection(geom.bounds)
            for pfi in lga_in_scope:
                lga_name, lga_geom = lga_geoms[pfi]
                if lga_geom.contains(geom):
                    lgas.append(lga_name)
                    
            lga_name = 'UNKNOWN'
            if len(lgas) == 1:
                lga_name = lgas[0]
            elif len(lgas) > 1:
                lga_name = sorted(lgas)[0]

            
            # self intersections
            # todo, check dependency if required
            intersect_count = 0

            sbc.add_row((addr_pfi,
                         locality_name, locality_percent,
                         x_vicgrid, y_vicgrid,
                         x_ingr, y_ingr,
                         x_ingr_uor, y_ingr_uor,
                         lga_name,
                         intersect_count))
            
            if enum % 1000 == 0:
                logging.info(enum)
            if enum % 100000 == 0:
                sbc.flush()
        logging.info(enum)
    logging.info('count start: {}'.format(sbc.count_start))
    logging.info('count finish: {}'.format(sbc.count_finish))


def calc_road_infrastructure_detail(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    ingr_sr = gis.ingr_spatial_reference()
    ingr_uor_sr = gis.ingr_uor_spatial_reference()


    logging.info('reading locality geoms')
    locality_geoms = {}
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'LOCALITY'),
                               field_names=['PFI', 'NAME', 'SHAPE@WKB']) as sc:
        for pfi, locality_name, wkb in sc:
            locality_geoms[pfi] = (locality_name, shapely.prepared.prep(shapely.wkb.loads(str(wkb))))
    logging.info(len(locality_geoms))

    logging.info('building locality rtree')
    def stream_load_locality():
        for pfi, (locality_name, geom) in locality_geoms.iteritems():
            yield (pfi, geom.context.bounds, None)
    locality_rtree = rtree.index.Index(stream_load_locality())
    
            
    logging.info('reading lga geoms')
    lga_geoms = {}
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'LGA'),
                               field_names=['PFI', 'NAME', 'SHAPE@WKB']) as sc:
        for pfi, lga_name, wkb in sc:
            lga_geoms[pfi] = (lga_name, shapely.prepared.prep(shapely.wkb.loads(str(wkb))))
    logging.info(len(lga_geoms))

    logging.info('building lga rtree')
    def stream_load_lga():
        for pfi, (lga_name, geom) in lga_geoms.iteritems():
            yield (pfi, geom.context.bounds, None)
    lga_rtree = rtree.index.Index(stream_load_lga())


    logging.info('looping ROAD_INFRASTRUCTURE...')
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD_INFRASTRUCTURE'),
                               field_names=['UFI', 'SHAPE@X', 'SHAPE@Y'],
                               sql_clause=(None, 'ORDER BY PFI')) as sc, \
         arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ADDRESS'),
                               field_names=['UFI', 'SHAPE@X', 'SHAPE@Y'],
                               spatial_reference=ingr_sr,
                               sql_clause=(None, 'ORDER BY UFI')) as sc_ingr, \
         dbpy.SQL_BULK_COPY(em.server, em.database_name, 'dbo.ROAD_INFRASTRUCTURE_DETAIL') as sbc:

        total_area = shapely.geometry.Point(0,0).buffer(2.5).area

        for enum, (row_vg, row_ingr) in enumerate(itertools.izip(sc, sc_ingr)):

            ri_ufi, x_vicgrid, y_vicgrid = row_vg
            _, x_ingr, y_ingr = row_ingr
            x_ingr_uor = x_ingr * 100.0
            y_ingr_uor = y_ingr * 100.0
            
            geom = shapely.geometry.Point(x_vicgrid, y_vicgrid)

            # locality
            localities = []
            locality_in_scope = locality_rtree.intersection(geom.bounds)
            for pfi in locality_in_scope:
                if locality_geoms[pfi][1].contains(geom):
                    localities.append(pfi)

            locality_name = 'UNKNOWN'
            locality_percent = 0.0
            if len(localities) == 1:
                locality_name = locality_geoms[localities[0]][0]
                locality_percent = 100.0
                
            elif len(localities) > 1:
                logging.info('within 2 localities: {}'.format(ri_ufi))
                localities_ranked = []
                
                # determine largest locality by area if contained within multiple
                for pfi in localities:
                    locality_name, locality_geom = locality_geoms[pfi]
                    geom_buffer = geom.buffer(2.5)
                    area_geom = locality_geom.context.intersection(geom_buffer)
                    locality_percent = area_geom.area / total_area
                    localities_ranked.append((pfi, locality_name, locality_geom, locality_percent))
                
                locality_name, locality_percent = max(localities_ranked, key=lambda x: x[-1])


            # lga
            lgas = []
            lga_in_scope = lga_rtree.intersection(geom.bounds)
            for pfi in lga_in_scope:
                lga_name, lga_geom = lga_geoms[pfi]
                if lga_geom.contains(geom):
                    lgas.append(lga_name)
                    
            lga_name = 'UNKNOWN'
            if len(lgas) == 1:
                lga_name = lgas[0]
            elif len(lgas) > 1:
                lga_name = sorted(lgas)[0]

            sbc.add_row((ri_ufi,
                         locality_name, locality_percent,
                         x_vicgrid, y_vicgrid,
                         x_ingr, y_ingr,
                         x_ingr_uor, y_ingr_uor,
                         lga_name))
            
            if enum % 1000 == 0:
                logging.info(enum)
            if enum % 100000 == 0:
                sbc.flush()
        logging.info(enum)
    logging.info('count start: {}'.format(sbc.count_start))
    logging.info('count finish: {}'.format(sbc.count_finish))


def create_address_gnaf_detail_table(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    sql_script = os.path.join(em.path, 'sql', 'detail_tables', 'create_address_gnaf_detail.sql')
    logging.info('running sql script: {}'.format(sql_script))

    dbpy.exec_script(em.server, em.database_name, sql_script)


def calc_address_gnaf_detail(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    ingr_sr = gis.ingr_spatial_reference()
    ingr_uor_sr = gis.ingr_uor_spatial_reference()


    logging.info('reading locality geoms')
    locality_geoms = {}
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'LOCALITY'),
                               field_names=['PFI', 'NAME', 'SHAPE@WKB']) as sc:
        for pfi, locality_name, wkb in sc:
            locality_geoms[pfi] = (locality_name, shapely.prepared.prep(shapely.wkb.loads(str(wkb))))
    logging.info(len(locality_geoms))

    logging.info('building locality rtree')
    


    def stream_load_locality():
        for pfi, (locality_name, geom) in locality_geoms.iteritems():
            yield (pfi, geom.context.bounds, None)
    locality_rtree = rtree.index.Index(stream_load_locality())
    
            
    logging.info('reading lga geoms')
    lga_geoms = {}
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'LGA'),
                               field_names=['PFI', 'NAME', 'SHAPE@WKB']) as sc:
        for pfi, lga_name, wkb in sc:
            lga_geoms[pfi] = (lga_name, shapely.prepared.prep(shapely.wkb.loads(str(wkb))))
    logging.info(len(lga_geoms))

    logging.info('building lga rtree')
    def stream_load_lga():
        for pfi, (lga_name, geom) in lga_geoms.iteritems():
            yield (pfi, geom.context.bounds, None)
    lga_rtree = rtree.index.Index(stream_load_lga())


    logging.info('looping ADDRESS_GNAF...')
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ADDRESS_GNAF'),
                               field_names=['ADDRESS_DETAIL_PID', 'SHAPE@X', 'SHAPE@Y'],
                               sql_clause=(None, 'ORDER BY ADDRESS_DETAIL_PID')) as sc, \
         arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ADDRESS_GNAF'),
                               field_names=['ADDRESS_DETAIL_PID', 'SHAPE@X', 'SHAPE@Y'],
                               spatial_reference=ingr_sr,
                               sql_clause=(None, 'ORDER BY ADDRESS_DETAIL_PID')) as sc_ingr, \
         dbpy.SQL_BULK_COPY(em.server, em.database_name, 'dbo.ADDRESS_GNAF_DETAIL') as sbc:

        total_area = shapely.geometry.Point(0,0).buffer(2.5).area

        for enum, (row_vg, row_ingr) in enumerate(itertools.izip(sc, sc_ingr)):

            addr_pfi, x_vicgrid, y_vicgrid = row_vg
            _, x_ingr, y_ingr = row_ingr
            x_ingr_uor = x_ingr * 100.0
            y_ingr_uor = y_ingr * 100.0
            
            geom = shapely.geometry.Point(x_vicgrid, y_vicgrid)

            # locality
            localities = []
            locality_in_scope = locality_rtree.intersection(geom.bounds)
            for pfi in locality_in_scope:
                if locality_geoms[pfi][1].contains(geom):
                    localities.append(pfi)

            locality_name = 'UNKNOWN'
            locality_percent = 0.0
            if len(localities) == 1:
                locality_name = locality_geoms[localities[0]][0]
                locality_percent = 100.0
                
            elif len(localities) > 1:
                logging.info('within 2 localities: {}'.format(addr_pfi))
                localities_ranked = []
                
                # determine largest locality by area if contained within multiple
                for pfi in localities:
                    locality_name, locality_geom = locality_geoms[pfi]
                    geom_buffer = geom.buffer(2.5)
                    area_geom = locality_geom.context.intersection(geom_buffer)
                    locality_percent = area_geom.area / total_area
                    localities_ranked.append((pfi, locality_name, locality_geom, locality_percent))
                
                locality_name, locality_percent = max(localities_ranked, key=lambda x: x[-1])


            # lga
            lgas = []
            lga_in_scope = lga_rtree.intersection(geom.bounds)
            for pfi in lga_in_scope:
                lga_name, lga_geom = lga_geoms[pfi]
                if lga_geom.contains(geom):
                    lgas.append(lga_name)
                    
            lga_name = 'UNKNOWN'
            if len(lgas) == 1:
                lga_name = lgas[0]
            elif len(lgas) > 1:
                lga_name = sorted(lgas)[0]

            
            # self intersections
            # todo, check dependency if required
            intersect_count = 0

            sbc.add_row((addr_pfi,
                         locality_name, locality_percent,
                         x_vicgrid, y_vicgrid,
                         x_ingr, y_ingr,
                         x_ingr_uor, y_ingr_uor,
                         lga_name,
                         intersect_count))
            
            if enum % 1000 == 0:
                logging.info(enum)
            if enum % 10000 == 0:
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
                
##                create_address_detail_table(estamap_version)
##                calc_address_detail(estamap_version)
##                
##                create_road_infrastructure_detail_table(estamap_version)
##                calc_road_infrastructure_detail(estamap_version)

                create_address_gnaf_detail_table(estamap_version)
                calc_address_gnaf_detail(estamap_version)
                
            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')

