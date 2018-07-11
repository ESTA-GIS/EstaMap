'''
ADDRESS Validation against ROAD.

Usage:
  address_road_validation.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  --where_clause <sql>    Where Clause to filter fc
  --log_file <file>       Log File name. [default: address_road_validation.log]
  --log_path <folder>     Folder to store the log file. [default: c:\\temp]
'''
import os
import sys
import time
import logging
import shutil
import re
import itertools
import multiprocessing
import math

import clr

from docopt import docopt
import arcpy
import lmdb
import shapely.wkb
import shapely.geometry
import shapely.prepared
import rtree
import zmq
from zmq.decorators import context, socket

import log
import dev as gis
import dbpy


class Validator(object):

    def __init__(self, estamap_version, category_code, rebuild=False, debug=False):
        self.estamap_version = estamap_version
        self.category_code = category_code

        self.em = gis.ESTAMAP(estamap_version)

        self.temp_path = 'c:\\temp\\address_road_validation_{}'.format(estamap_version)
        self.temp_lmdb = os.path.join(self.temp_path, 'address_road_lmdb')

        self.rtree_road_location = rtree_road_location = os.path.join(self.temp_path, 'road_rtree')
        self.rtree_road_e_location = rtree_road_e_location = os.path.join(self.temp_path, 'road_exclude_unnamed_rtree')
        self.rtree_locality_location = rtree_locality_locality = os.path.join(self.temp_path, 'locality_rtree')

        self.debug = debug
        self.rebuild = rebuild

        # setup temp path
        if not os.path.exists(self.temp_path):
            logging.info('creating temp_path: {}'.format(self.temp_path))
            os.makedirs(self.temp_path)

        #
        # get validation rules
        # 
        cursor = self.em.conn.cursor()
        
        category_code, category_desc = cursor.execute('select CATEGORY_CODE, CATEGORY_DESCRIPTION from VALIDATION_CATEGORY').fetchone()

        self.rule_codes = []
        for rule_code, test_order in cursor.execute("select RULE_CODE, TEST_ORDER from VALIDATION_CATEGORY_RULE where CATEGORY_CODE = '{}' order by TEST_ORDER".format(category_code)):
            self.rule_codes.append([rule_code, test_order])

        self.rules = []
        for rule_code, test_order in self.rule_codes:
            (rule_desc,
             score,
             filter_attr,
             match_address,
             match_spatial,
             match_attribute,
             max_intersects,
             max_distance) = cursor.execute('''
                select 
                    DESCRIPTION,
                    SCORE,
                    FILTER_ATTRIBUTE,
                    MATCH_ADDRESS,
                    MATCH_SPATIAL,
                    MATCH_ATTRIBUTE,
                    MAX_INTERSECTS,
                    MAX_DISTANCE
                from VALIDATION_RULE
                where CODE='{code}'
            '''.format(code=rule_code)).fetchone()
            
            self.rules.append(ValidationRule(category_code,
                                             category_desc,
                                             rule_code,
                                             rule_desc,
                                             test_order,
                                             score,
                                             filter_attr,
                                             match_address,
                                             match_spatial,
                                             match_attribute,
                                             max_intersects,
                                             max_distance))
        
        if not self.rebuild:
            
            self.env = env = lmdb.Environment(path=self.temp_lmdb,
                                              map_size=2000000000,
                                              readonly=False,
                                              max_dbs=10)
            self.road_db = road_db = env.open_db('road_db')
            self.road_geom_db = road_geom_db = env.open_db('road_geom_db')
            self.road_alias_db = road_alias_db = env.open_db('road_alias_db', dupsort=True)
            self.road_name_db = road_name_db = env.open_db('road_name_db')
            self.locality_db = locality_db = env.open_db('locality_db')
            self.locality_geom_db = locality_geom_db = env.open_db('locality_geom_db')

            self.rtree_roads = rtree.Rtree(self.rtree_road_location)
            self.rtree_roads_e = rtree.Rtree(self.rtree_road_e_location)
            self.rtree_locality = rtree.Rtree(self.rtree_locality_location)
       
        else:
            
            #
            # setup temp lmdb
            #
            if os.path.exists(self.temp_lmdb):
                shutil.rmtree(self.temp_lmdb)
                
            logging.info('setup temp lmdb')
            self.env = env = lmdb.Environment(path=self.temp_lmdb,
                                              map_size=2000000000,
                                              readonly=False,
                                              max_dbs=10)
            self.road_db = road_db = env.open_db('road_db')
            self.road_geom_db = road_geom_db = env.open_db('road_geom_db')
            self.road_alias_db = road_alias_db = env.open_db('road_alias_db', dupsort=True)
            self.road_name_db = road_name_db = env.open_db('road_name_db')
            self.locality_db = locality_db = env.open_db('locality_db')
            self.locality_geom_db = locality_geom_db = env.open_db('locality_geom_db')


            logging.info('loading road')
            with env.begin(write=True, db=road_db) as road_txn, \
                 arcpy.da.SearchCursor(in_table=os.path.join(self.em.sde, 'ROAD_VALIDATED'),
                                       field_names=['PFI',
                                                    'LEFT_LOCALITY',
                                                    'RIGHT_LOCALITY',
                                                    ],
                                       sql_clause=(None, 'ORDER BY PFI')) as sc:
                for enum, row in enumerate(sc, 1):
                    record = [str(f) for f in row[1:]]
                    road_txn.put(str(row[0]), ','.join(record))
                    if enum % 100000 == 0:
                        logging.info(enum)
                logging.info(enum)


            logging.info('load road geom')
            with env.begin(write=True, db=road_geom_db) as road_geom_txn, \
                 arcpy.da.SearchCursor(in_table=os.path.join(self.em.sde, 'ROAD_VALIDATED'),
                                       field_names=['PFI', 'SHAPE@WKB',],
                                       sql_clause=(None, 'ORDER BY PFI')) as sc:
                for enum, (pfi, wkb,) in enumerate(sc, 1):
                    road_geom_txn.put(str(pfi), str(wkb))
                    if enum % 100000 == 0:
                        logging.info(enum)
                logging.info(enum)


            logging.info('load road alias')
            with env.begin(write=True, db=road_alias_db) as road_alias_txn, \
                 arcpy.da.SearchCursor(in_table=os.path.join(self.em.sde, 'ROAD_ALIAS'),
                                       field_names=['PFI', 'ROAD_NAME_ID', 'ALIAS_NUMBER', 'ROUTE_FLAG',],
                                       sql_clause=(None, 'ORDER BY PFI')) as sc:
                for enum, (pfi, rnid, alias_num, route_flag) in enumerate(sc, 1):
                    record = [str(f) for f in (rnid, alias_num, route_flag)]
                    road_alias_txn.put(str(pfi), ','.join(record))
                    if enum % 100000 == 0:
                        logging.info(enum)
                logging.info(enum)

            logging.info('load road name register')
            with env.begin(write=True, db=road_name_db) as road_name_txn, \
                 arcpy.da.SearchCursor(in_table=os.path.join(self.em.sde, 'ROAD_NAME_REGISTER'),
                                       field_names=['ROAD_NAME_ID', 'ROAD_NAME', 'ROAD_TYPE', 'ROAD_SUFFIX', 'SOUNDEX', 'ROUTE_FLAG'],
                                       sql_clause=(None, 'ORDER BY ROAD_NAME_ID')) as sc:
                for enum, (rnid, road_name, road_type, road_suffix, sdx, route_flag) in enumerate(sc, 1):
                    record = [str(f) for f in (road_name, road_type, road_suffix, sdx, route_flag)]
                    road_name_txn.put(str(rnid), ','.join(record))
                    if enum % 100000 == 0:
                        logging.info(enum)
                logging.info(enum)

            logging.info('load locality')
            with env.begin(write=True, db=locality_db) as locality_txn, \
                 arcpy.da.SearchCursor(in_table=os.path.join(self.em.sde, 'LOCALITY'),
                                       field_names=['PFI', 'NAME',],
                                       sql_clause=(None, 'ORDER BY PFI')) as sc:
                for enum, (pfi, locality_name,) in enumerate(sc, 1):
                    locality_txn.put(str(pfi), str(locality_name))
                    locality_txn.put(str(locality_name), str(pfi))
                    if enum % 100000 == 0:
                        logging.info(enum)
                logging.info(enum)

            logging.info('load locality geom')
            with env.begin(write=True, db=locality_geom_db) as locality_geom_txn, \
                 arcpy.da.SearchCursor(in_table=os.path.join(self.em.sde, 'LOCALITY'),
                                       field_names=['PFI', 'NAME', 'SHAPE@WKB',],
                                       sql_clause=(None, 'ORDER BY PFI')) as sc:
                for enum, (pfi, locality_name, wkb,) in enumerate(sc, 1):
                    locality_geom_txn.put(str(pfi), str(wkb))
                    locality_geom_txn.put(str(locality_name), str(wkb))
                    if enum % 100000 == 0:
                        logging.info(enum)
                logging.info(enum)
                

            # 
            # setup rtrees
            #
            if os.path.exists(self.rtree_road_location + '.dat'):
                os.remove(self.rtree_road_location + '.dat')
                os.remove(self.rtree_road_location + '.idx')
            if os.path.exists(self.rtree_road_e_location + '.dat'):
                os.remove(self.rtree_road_e_location + '.dat')
                os.remove(self.rtree_road_e_location + '.idx')
            if os.path.exists(self.rtree_locality_location + '.dat'):
                os.remove(self.rtree_locality_location + '.dat')
                os.remove(self.rtree_locality_location + '.idx')
            
            logging.info('setup and build road rtree')
            with self.env.begin(db=self.road_geom_db) as road_geom_txn:
                
                def bulk_load_road():
                    geom_cursor = road_geom_txn.cursor()
                    for yielded, (pfi, wkb) in enumerate(geom_cursor.iternext(), 1):
                        geom = shapely.wkb.loads(wkb)
                        yield (int(pfi), geom.bounds, None)
                        if yielded % 100000 == 0:
                            logging.info(yielded)
                    logging.info(yielded)

                self.rtree_roads = rtree.Rtree(self.rtree_road_location, bulk_load_road())
                self.rtree_roads.close()
                self.rtree_roads = rtree.Rtree(self.rtree_road_location)


            logging.info('setup and build road exclude unnamed rtree')
            with self.env.begin(db=self.road_alias_db) as road_alias_txn, \
                 self.env.begin(db=self.road_geom_db) as road_geom_txn, \
                 self.env.begin(db=self.road_name_db) as road_name_txn:

                def bulk_load_road_excl_unnamed():

                    road_alias_cursor = road_alias_txn.cursor()
                    geom_cursor = road_geom_txn.cursor()
                    road_name_cursor = road_name_txn.cursor()

                    yielded = 0
                    geom_cursor.first()
                    for pfi, wkb in geom_cursor.iternext():

                        geom = shapely.wkb.loads(wkb)
                        road_alias_cursor.set_key(pfi)

                        exclude_road = False
                        for ra_record in road_alias_cursor.iternext_dup():
                            rnid, alias_num, route_flag = ra_record.split(',')

                            road_name, road_type, road_suffix, sdx, route_flag = road_name_cursor.get(rnid).split(',')

                            if road_name in ('UNNAMED', 'UNKNOWN'):
                                exclude_road = True
                                break

                        if exclude_road:
                            continue
                        
                        yield (int(pfi), geom.bounds, None)
                        yielded = yielded + 1

                        if yielded % 100000 == 0:
                            logging.info(yielded)
                    logging.info(yielded)
                    
                logging.info('loading roads excluding unnamed, unknown, routeflag')
                self.rtree_roads_e = rtree.Rtree(self.rtree_road_e_location, bulk_load_road_excl_unnamed())
                self.rtree_roads_e.close()
                self.rtree_roads_e = rtree.Rtree(self.rtree_road_e_location)


            logging.info('setup and build locality rtree')
            with self.env.begin(db=locality_geom_db) as locality_geom_txn:

                locality_geom_cursor = locality_geom_txn.cursor()
                
                def bulk_load_locality():
                    
                    for pfi, wkb in locality_geom_cursor.iternext():
                        try:
                            int(pfi)
                        except:
                            continue

                        geom = shapely.wkb.loads(wkb)

                        yield (int(pfi), geom.bounds, None)
                        
                self.rtree_locality = rtree.Rtree(self.rtree_locality_location, bulk_load_locality())
                self.rtree_locality.close()
                self.rtree_locality = rtree.Rtree(self.rtree_locality_location)

        # prepare locality geoms
        logging.info('preparing locality geoms')
        self.locality_geoms_prepared = {}
        with self.env.begin(db=locality_geom_db) as locality_geom_txn:
            
            locality_geom_cursor = locality_geom_txn.cursor()
            locality_geom_cursor.first()
            
            for locality_name, wkb in locality_geom_cursor.iternext():
                try:
                    int(locality_name)
                    continue
                except:
                    pass
                self.locality_geoms_prepared[locality_name] = shapely.prepared.prep(shapely.wkb.loads(wkb))

    def validate(self,
                 x,
                 y,
                 road_name,
                 road_type,
                 road_suffix,
                 rnid,
                 soundex,
                 locality_name
                 ):
        with self.env.begin(db=self.road_db) as road_txn, \
             self.env.begin(db=self.road_name_db) as road_name_txn, \
             self.env.begin(db=self.road_alias_db) as road_alias_txn, \
             self.env.begin(db=self.road_geom_db) as road_geom_txn, \
             self.env.begin(db=self.locality_db) as locality_txn, \
             self.env.begin(db=self.locality_geom_db) as locality_geom_txn:

            road_cursor = road_txn.cursor()
            road_name_cursor = road_name_txn.cursor()
            road_alias_cursor = road_alias_txn.cursor()
            road_geom_cursor = road_geom_txn.cursor()
            locality_cursor = locality_txn.cursor()
            locality_geom_cursor = locality_geom_txn.cursor()

            results = []
            for enum_rule, rule in enumerate(self.rules, 1):

                # roads in scope are within distance (metres) of:
                # - rule max distance OR
                # - steps of buffer_increments value of max_distance
                # - AND intersect locality geom
                buffer_increments = 250
                roads_in_scope_all = set()
                is_attr_valid = int(False)
                for buffer_value in itertools.chain(xrange(min(rule.max_distance, buffer_increments), rule.max_distance, buffer_increments), [rule.max_distance]):

                    geom_scope = shapely.geometry.Point(x, y).buffer(buffer_value)
                    geom_scope_prepped = shapely.prepared.prep(geom_scope)

                    # count roads in scope
                    roads_ranked = []
                    count_roads_in_scope = self.rtree_roads_e.count(geom_scope.bounds)
##                    logging.debug('{} num roads in scope: {}'.format(rule.code, count_roads_in_scope))
                    if count_roads_in_scope > 0:

                        #
                        # 1. get distances and roads in scope
                        #
                        for enum_road, road_pfi_in_scope in enumerate(self.rtree_roads_e.intersection(geom_scope.bounds), 1):
                            road_pfi_in_scope = str(road_pfi_in_scope)
                            
                            # skip road if already been measured
                            if road_pfi_in_scope in roads_in_scope_all:
                                continue

                            geom_line = shapely.wkb.loads(road_geom_cursor.get(road_pfi_in_scope))
                            
                            if geom_scope_prepped.intersects(geom_line):

                                # road must intersect address locality
                                intersects_locality = False
                                if rule.code == 'F_N' or locality_name == 'UNKNOWN':
                                    intersects_locality = True
                                else:
                                    if self.locality_geoms_prepared.has_key(locality_name):
                                        if self.locality_geoms_prepared[locality_name].intersects(geom_line):
                                            intersects_locality = True

                                if not intersects_locality:
                                    continue

                                roads_in_scope_all.add(road_pfi_in_scope)

                                dist_along_road = geom_line.project(shapely.geometry.Point(x, y))
                                pt_interpolated = geom_line.interpolate(dist_along_road)

                                geom_address_road = shapely.geometry.LineString([(x,y), (pt_interpolated.x, pt_interpolated.y)])
                                
                                dist_from_road = geom_address_road.length

                                roads_ranked.append((road_pfi_in_scope, dist_from_road, dist_along_road, geom_address_road))
                        
                        # 
                        # 2. sort roads by dist and validate
                        #
                        roads_ranked.sort(key=lambda rr: rr[1])
                        if len(roads_ranked) == 0: continue
                        closest_dist = roads_ranked[0][1]
##                        if self.debug: logging.debug('{} num roads ranked: {}'.format(rule.code, len(roads_ranked)))
                        for road_rank, (road_pfi, dist_from_road, dist_along_road, geom_address_road) in enumerate(roads_ranked, 1):

##                            if rule.match_spatial == 'Nearest' and dist_from_road > closest_dist:
##                                break

                            test_road_left_locality, test_road_right_locality = road_cursor.get(road_pfi).split(',')

                            #
                            # attribute validation
                            #
                            road_alias_cursor.set_key(road_pfi)
                            for enum_ra, ra_record in enumerate(road_alias_cursor.iternext_dup()):
                                test_road_rnid, test_road_alias_num, test_road_route_flag = ra_record.split(',')
                                test_road_name, test_road_type, test_road_suffix, test_road_sdx, test_road_route_flag = road_name_cursor.get(test_road_rnid).split(',')

##                                print road_pfi, repr(test_road_rnid), repr(rnid), test_road_rnid == rnid,

                                is_attr_valid = int(rule.validate_attribute(road_name,
                                                                            road_type,
                                                                            road_suffix,
                                                                            rnid,
                                                                            soundex,
                                                                            test_road_name,
                                                                            test_road_type,
                                                                            test_road_suffix,
                                                                            test_road_rnid,
                                                                            test_road_sdx,
                                                                            ))
##                                print is_attr_valid,
##                                print locality_name.lower() ,  [test_road_left_locality.lower(), test_road_right_locality.lower(), 'unknown', ] 
                            
##                                # road must be within locality
##                                if locality_name.lower() in [test_road_left_locality.lower(), test_road_right_locality.lower(), 'unknown', ] or \
##                                   rule.code == 'F_N':
##                                    test_road_locality_name = locality_name
##                                    is_attr_valid = is_attr_valid * 1
##                                else:
##                                    test_road_locality_name = ''
##                                    is_attr_valid = 0
                                
                                if is_attr_valid:
                                    break
                            
                            #
                            # spatial validation
                            # 
                            is_spatial_valid = int(False)
                            roads_intersect = set()
                            side_of_road = ''
                            if is_attr_valid:

                                # count roads intersecting address_road
                                count_intersect_in_scope = self.rtree_roads.count(geom_address_road.bounds)
                                if count_intersect_in_scope > 0:

                                    # get roads intersecting address_road
                                    for road_intersect_in_scope in self.rtree_roads.intersection(geom_address_road.bounds):
                                        road_intersect_in_scope = str(road_intersect_in_scope)

                                        if geom_address_road.crosses(shapely.wkb.loads(road_geom_cursor.get(road_intersect_in_scope))):
                                            roads_intersect.add(road_intersect_in_scope)
                                roads_intersect.discard(str(road_pfi))  # exclude the road_pfi we are measuring to

                                # count roads crossing address_road
                                crosses_count = 0.0
                                for road_intersect in roads_intersect:
                                    road_intersect_rnid, road_intersect_aliasnum, road_intersect_routeflag = road_alias_cursor.get(road_intersect).split(',')
                                    road_intersect_name, road_intersect_type, road_intersect_suffix, road_intersect_sdx, road_intersect_route_flag = road_name_cursor.get(road_intersect_rnid).split(',')
                                    if road_intersect_name in ('UNNAMED', 'UNKNOWN'):
                                        crosses_count = crosses_count + 0.4
                                    else:
                                        crosses_count = crosses_count + 1

                                is_spatial_valid = int(rule.validate_spatial(dist_from_road, int(crosses_count)))

                            test_road_locality_name = locality_name
                            if is_attr_valid and is_spatial_valid:

                                road_geom_line = shapely.wkb.loads(road_geom_cursor.get(str(road_pfi)))
                                
                                # determine side of road
                                side_of_road = get_side_of_line(point=shapely.geometry.Point(x, y), line=road_geom_line)

                                # if geom_address_road is smaller than the feature dataset tolerance it will cause an error
                                # thus increase the geometry length
                                if geom_address_road.length < 0.01:
                                    dx = geom_address_road.coords[-1][0] - geom_address_road.coords[0][0]
                                    dy = geom_address_road.coords[-1][1] - geom_address_road.coords[0][1]

                                    linelen = math.hypot(dx, dy)
                                    if linelen == 0.0:
                                        linelen = 1.0

                                    x3 = geom_address_road.coords[-1][0] + dx/linelen * .1
                                    y3 = geom_address_road.coords[-1][1] + dy/linelen * .1
                                    geom_address_road = shapely.geometry.LineString([(x, y), (x3, y3)])
                                
                                result = [road_pfi, enum_rule, rule.code, rule.test_score, is_attr_valid, is_spatial_valid, rnid, test_road_rnid, locality_name, test_road_locality_name, soundex, test_road_sdx, len(roads_intersect), side_of_road, dist_from_road, dist_along_road, geom_address_road.wkt]
                                results.append(result)
                                return results

                            result = [road_pfi, enum_rule, rule.code, rule.test_score, is_attr_valid, is_spatial_valid, rnid, test_road_rnid, locality_name, test_road_locality_name, soundex, test_road_sdx, len(roads_intersect), side_of_road, dist_from_road, dist_along_road, geom_address_road.wkt]
                            results.append(result)
                            if rule.match_spatial == 'Nearest' and dist_from_road > closest_dist:
                                break

                            if is_attr_valid:
                                break
                        if is_attr_valid:
                            break
                    if is_attr_valid:
                        break

def get_side_of_line(point, line):

    dist_along_line = line.project(point)
    if dist_along_line < 1:
        dist_along_line_0 = 0
        dist_along_line_1 = 1
        dist_along_line_2 = 2
    elif dist_along_line > line.length - 1:
        dist_along_line_0 = line.length - 2
        dist_along_line_1 = line.length - 1
        dist_along_line_2 = line.length
    else:
        dist_along_line_0 = dist_along_line - 1
        dist_along_line_1 = dist_along_line
        dist_along_line_2 = dist_along_line + 1

    pt0 = line.interpolate(dist_along_line_0)
    pt1 = line.interpolate(dist_along_line_1)
    pt2 = line.interpolate(dist_along_line_2)

    lr_coords = list(pt0.coords) + list(pt1.coords) + list(pt2.coords) + list(point.coords) + list(pt0.coords)

    is_left_side = shapely.geometry.LinearRing(lr_coords).is_ccw

    return 'L' if is_left_side else 'R'


class ValidationRule(object):

    def __init__(self,
                 CATEGORY_CODE,
                 CATEGORY_DESCRIPTION,
                 CODE,
                 DESCRIPTION,
                 TEST_ORDER,
                 SCORE,
                 FILTER_ATTRIBUTE,
                 MATCH_ADDRESS,
                 MATCH_SPATIAL,
                 MATCH_ATTRIBUTE,
                 MAX_INTERSECTS,
                 MAX_DISTANCE):

        self.category_code = CATEGORY_CODE
        self.category_description = CATEGORY_DESCRIPTION

        self.code = CODE
        self.description = DESCRIPTION
       
        self.test_order = int(TEST_ORDER)
        self.test_score = int(SCORE)

        self.match_attribute = MATCH_ATTRIBUTE
        self.match_address = MATCH_ADDRESS
        self.match_spatial = MATCH_SPATIAL
        self.filter_attribute = FILTER_ATTRIBUTE
        
        self.max_distance = int(MAX_DISTANCE)
        self.max_intersects = int(MAX_INTERSECTS)

        self.re_partial = re.compile(r"-| ")

    def validate_attribute(self,
                           base_road_name,
                           base_road_type,
                           base_road_suffix,
                           base_rnid,
                           base_soundex,
                           test_road_name,
                           test_road_type,
                           test_road_suffix,
                           test_rnid,
                           test_soundex):
##        print self.code, self.match_attribute,
        if self.match_attribute == 'RoadNameID':
##            print int(base_rnid) == int(test_rnid)
            if int(base_rnid) == int(test_rnid):
                return True

        elif self.match_attribute == 'RoadNameType':
            if base_road_name == test_road_name and \
               base_road_type == test_road_type:
                return True
        
        elif self.match_attribute == 'RoadName':
            if base_road_name == test_road_name:
                return True

        elif self.match_attribute == 'NoSpace':
            if base_road_name == test_road_name:
                return False
            
            if base_road_name.replace(' ', '').replace('-', '') == test_road_name.replace(' ', '').replace('-', ''):
                return True

        elif self.match_attribute == 'OldPrefix':
            if base_road_name.startswith('OLD ') or test_road_name.startswith('OLD '):
                if base_road_name.replace('OLD ', '') == test_road_name.replace('OLD ', ''):
                    return True

        elif self.match_attribute == 'SSuffix':
            if base_road_name.endswith('S') or test_road_name.endswith('S'):
                base = base_road_name[:-1] if base_road_name.endswith('S') else base_road_name
                test = test_road_name[:-1] if test_road_name.endswith('S') else test_road_name
                if base == test:
                    return True

        elif self.match_attribute == 'StartsWith':
            if base_road_name.startswith(test_road_name[:5]):
                return True

        elif self.match_attribute == 'Partial':
##            if base_road_type == test_road_type:
            base_parts = self.re_partial.split(base_road_name)
            test_parts = self.re_partial.split(test_road_name)

            matched = 0.0
            for base_part in base_parts:
                for test_part in test_parts:

                    if base_part == test_part:
                       matched = matched + 1

            if matched / min(len(base_parts), len(test_parts)) >= 0.3:
                return True

        elif self.match_attribute == 'Soundex':
            if base_soundex == test_soundex:
                return True

        elif self.match_attribute == 'Named':
            if base_road_name <> 'UNNAMED':
                return True

        elif self.match_attribute == 'None':
            return True

        return False


    def validate_spatial(self, distance_from_road, intersect_count):
        
        if self.code == 'F_N':
            return True
        elif distance_from_road < self.max_distance:

            if intersect_count <= self.max_intersects:
                return True
            
            else:
                if self.match_spatial == 'Nearest':
                    return True
                
##                elif intersect_count <= self.max_intersects + 3:
##                    return True
                elif intersect_count <= self.max_intersects:
                    return True

        return False


@context()
@socket(zmq.SUB)
@socket(zmq.REQ)
@socket(zmq.PULL)
@socket(zmq.PUSH)
def ValidatorWorker(estamap_version, category_code,
                    cmd_addr, sync_addr, work_addr, result_addr, 
                    ctx, cmder, syncer, worker, resulter):

    cmder.connect(cmd_addr)
    cmder.setsockopt(zmq.SUBSCRIBE, '')
    syncer.connect(sync_addr)
    worker.connect(work_addr)
    resulter.connect(result_addr)

    poller = zmq.Poller()
    poller.register(cmder, zmq.POLLIN)
    poller.register(worker, zmq.POLLIN)

    rules = Validator(estamap_version, category_code, rebuild=False)

    syncer.send(str(os.getpid()))
    syncer.recv()

    while True:
        socks = dict(poller.poll(1000))
        
        if socks.get(worker) == zmq.POLLIN:
            
            work = worker.recv_pyobj()
            pfi, x, y, road_name, road_type, road_suffix, rnid, soundex, locality_name = work
            results = rules.validate(x=x,
                                     y=y,
                                     road_name=road_name,
                                     road_type=road_type,
                                     road_suffix=road_suffix,
                                     rnid=rnid,
                                     soundex=soundex,
                                     locality_name=locality_name)
            resulter.send_pyobj((pfi, results))

        if socks.get(cmder) == zmq.POLLIN:
            break


def validate_address_mp(estamap_version, where_clause=None):

    logging.info('environment')
    category_code = 'ADDRESS_ROAD'
    em = gis.ESTAMAP(estamap_version)
    v = Validator(estamap_version, category_code, rebuild=True)
    temp_fgdb = 'c:\\temp\\address_validation_{}.gdb'.format(estamap_version)
    temp_arv_fc = os.path.join(temp_fgdb, 'ADDRESS_ROAD_VALIDATION')
    num_processes = multiprocessing.cpu_count()
    
    
    logging.info('clr')
    clr.AddReference('System')
    clr.AddReference('System.Data')
    clr_sqlserver_path = r'C:\Program Files (x86)\Microsoft SQL Server\130\SDK\Assemblies'
    if clr_sqlserver_path not in sys.path:
        sys.path.append(clr_sqlserver_path)
    clr.AddReference('Microsoft.SqlServer.Types')
    sql_geom = clr.Microsoft.SqlServer.Types.SqlGeometry()
    

    logging.info('creating validation fc')
    dbpy.exec_script(em.server, em.database_name, os.path.join(em.path, 'SQL', 'validation', 'create_address_road_validation.sql'))

    logging.info('creating temp fgdb')
    if arcpy.Exists(temp_fgdb):
        arcpy.Delete_management(temp_fgdb)
    arcpy.CreateFileGDB_management(*os.path.split(temp_fgdb))

    logging.info('creating temp validation fc')
    arcpy.CreateFeatureclass_management(out_path=temp_fgdb,
                                        out_name='ADDRESS_ROAD_VALIDATION_ALL',
                                        geometry_type='POLYLINE',
                                        template=os.path.join(em.sde, 'ADDRESS_ROAD_VALIDATION'),
                                        spatial_reference=arcpy.SpatialReference(3111))
    

    logging.info('setting up workers')
    context = zmq.Context(1)

    socket_cmd = context.socket(zmq.PUB)
    cmd_port = socket_cmd.bind_to_random_port('tcp://127.0.0.1')
    cmd_addr = 'tcp://127.0.0.1:{port}'.format(port=cmd_port)

    socket_sync = context.socket(zmq.REP)
    sync_port = socket_sync.bind_to_random_port('tcp://127.0.0.1')
    sync_addr = 'tcp://127.0.0.1:{port}'.format(port=sync_port)

    socket_work = context.socket(zmq.PUSH)
    socket_work.set_hwm(100000)
    work_port = socket_work.bind_to_random_port('tcp://127.0.0.1')
    work_addr = 'tcp://127.0.0.1:{port}'.format(port=work_port)

    socket_result = context.socket(zmq.PULL)
    socket_result.set_hwm(100000)
    result_port = socket_result.bind_to_random_port('tcp://127.0.0.1')
    result_addr = 'tcp://127.0.0.1:{port}'.format(port=result_port)

    poller = zmq.Poller()
    poller.register(socket_result, zmq.POLLIN)

    processes = []
    for num in range(num_processes):
        p = multiprocessing.Process(target=ValidatorWorker, args=(estamap_version, category_code,
                                                                  cmd_addr, sync_addr, work_addr, result_addr))
        p.start()
        processes.append(p)
    for num in range(num_processes):
        worker_id = socket_sync.recv()
        logging.info('worker started: {}'.format(worker_id))
        socket_sync.send('OK')


    def work_generator(estamap_version, where_clause=where_clause):
        em = gis.ESTAMAP(estamap_version)
        with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ADDRESS'),
                                   field_names=['PFI',
                                                'SHAPE@X',
                                                'SHAPE@Y',
                                                'ROAD_NAME',
                                                'ROAD_TYPE',
                                                'ROAD_SUFFIX',
                                                'LOCALITY_NAME'],
                                   where_clause="ADDRESS_CLASS <> 'M' and ISNULL(FEATURE_QUALITY_ID,'') <> 'PAPER_ROAD_ONLY'" + \
                                                (' AND ' + where_clause if where_clause else ''),
                                   sql_clause=(None, 'ORDER BY PFI')) as sc:
            for enum_address, row in enumerate(sc):

                pfi, x, y, road_name, road_type, road_suffix, locality_name = row

                # validate address values
                road_name, road_type, road_suffix, route_flag = em.parse_roadname(road_name, road_type, road_suffix, 0)    
                
                soundex = gis.generate_soundex(road_name)
                rnid = em.check_roadname(road_name, road_type, road_suffix)[1]
                yield pfi, x, y, road_name, road_type, road_suffix, rnid, soundex, locality_name

    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'ADDRESS_ROAD_VALIDATION') as sbc, \
         arcpy.da.InsertCursor(in_table=os.path.join(temp_fgdb, 'ADDRESS_ROAD_VALIDATION_ALL'),
                               field_names=['ADDR_PFI',
                                            'ROAD_PFI',
                                            'RULE_RANK',
                                            'RULE_CODE',
                                            'RULE_SCORE',
                                            'VALID_ATTR',
                                            'VALID_SPATIAL',
                                            'ADDR_RNID',
                                            'ROAD_RNID',
                                            'ADDR_LOCALITY_NAME',
                                            'ROAD_LOCALITY_NAME',
                                            'ADDR_SOUNDEX',
                                            'ROAD_SOUNDEX',
                                            'INTERSECTS',
                                            'SIDE_OF_ROAD',
                                            'DIST_FROM_ROAD',
                                            'DIST_ALONG_ROAD',
                                            'Shape@WKT']) as ic:
        work_gen = work_generator(estamap_version)
        num_work = 0
        num_results = 0
        total_results = 0
        work_gen_complete = False
        while True:

            try:
                work = next(work_gen)
                socket_work.send_pyobj(work)
                num_work = num_work + 1
            except StopIteration:
                work_gen_complete = True

            socks = dict(poller.poll(10))
          
            if socks.get(socket_result) == zmq.POLLIN:
                work_result = socket_result.recv_pyobj()
                num_results = num_results + 1

                pfi, results = work_result
                for result in results:
                    
                    ic.insertRow([pfi,] + result)
##                    sbc_all.add_row([pfi,] + result[:-1] + [geom,])
                    total_results = total_results + 1

                geom = sql_geom.Parse(clr.System.Data.SqlTypes.SqlString(result[-1]))
                geom.set_STSrid(clr.System.Data.SqlTypes.SqlInt32(3111))
                sbc.add_row([pfi,] + result[:-1] + [geom,])

                if num_results % 10000 == 0:
                    logging.info('{}'.format((num_results, total_results)))
                    sbc.flush()
##                    sbc_all.flush()
                if work_gen_complete and num_work == num_results:
                    logging.info('{}'.format((num_results, total_results)))
                    sbc.flush()
##                    sbc_all.flush()
                    break

        socket_cmd.send('finish')
        # close processes
        for p in processes:
            p.join(1)
            p.terminate()


def validate_address_gnaf_mp(estamap_version, where_clause=None):

    logging.info('environment')
    category_code = 'ADDRESS_ROAD'
    em = gis.ESTAMAP(estamap_version)
    v = Validator(estamap_version, category_code, rebuild=True)
    temp_fgdb = 'c:\\temp\\address_gnaf_validation_{}.gdb'.format(estamap_version)
    temp_arv_fc = os.path.join(temp_fgdb, 'ADDRESS_GNAF_ROAD_VALIDATION')
    num_processes = multiprocessing.cpu_count()
    
    
    logging.info('clr')
    clr.AddReference('System')
    clr.AddReference('System.Data')
    clr_sqlserver_path = r'C:\Program Files (x86)\Microsoft SQL Server\130\SDK\Assemblies'
    if clr_sqlserver_path not in sys.path:
        sys.path.append(clr_sqlserver_path)
    clr.AddReference('Microsoft.SqlServer.Types')
    sql_geom = clr.Microsoft.SqlServer.Types.SqlGeometry()
    

    logging.info('creating validation fc')
    dbpy.exec_script(em.server, em.database_name, os.path.join(em.path, 'SQL', 'validation', 'create_address_gnaf_road_validation.sql'))

    logging.info('creating temp fgdb')
    if arcpy.Exists(temp_fgdb):
        arcpy.Delete_management(temp_fgdb)
    arcpy.CreateFileGDB_management(*os.path.split(temp_fgdb))

    logging.info('creating temp validation fc')
    arcpy.CreateFeatureclass_management(out_path=temp_fgdb,
                                        out_name='ADDRESS_GNAF_ROAD_VALIDATION',
                                        geometry_type='POLYLINE',
                                        template=os.path.join(em.sde, 'ADDRESS_GNAF_ROAD_VALIDATION'),
                                        spatial_reference=arcpy.SpatialReference(3111))
    

    logging.info('setting up workers')
    context = zmq.Context(1)

    socket_cmd = context.socket(zmq.PUB)
    cmd_port = socket_cmd.bind_to_random_port('tcp://127.0.0.1')
    cmd_addr = 'tcp://127.0.0.1:{port}'.format(port=cmd_port)

    socket_sync = context.socket(zmq.REP)
    sync_port = socket_sync.bind_to_random_port('tcp://127.0.0.1')
    sync_addr = 'tcp://127.0.0.1:{port}'.format(port=sync_port)

    socket_work = context.socket(zmq.PUSH)
    socket_work.set_hwm(100000)
    work_port = socket_work.bind_to_random_port('tcp://127.0.0.1')
    work_addr = 'tcp://127.0.0.1:{port}'.format(port=work_port)

    socket_result = context.socket(zmq.PULL)
    socket_result.set_hwm(100000)
    result_port = socket_result.bind_to_random_port('tcp://127.0.0.1')
    result_addr = 'tcp://127.0.0.1:{port}'.format(port=result_port)

    poller = zmq.Poller()
    poller.register(socket_result, zmq.POLLIN)

    processes = []
    for num in range(num_processes):
        p = multiprocessing.Process(target=ValidatorWorker, args=(estamap_version, category_code,
                                                                  cmd_addr, sync_addr, work_addr, result_addr))
        p.start()
        processes.append(p)
    for num in range(num_processes):
        worker_id = socket_sync.recv()
        logging.info('worker started: {}'.format(worker_id))
        socket_sync.send('OK')


    def work_generator(estamap_version, where_clause=where_clause):
        em = gis.ESTAMAP(estamap_version)
        cursor = dbpy.create_conn_pyodbc(em.server, em.database_name)
        with cursor.execute('''
            SELECT
                A.ADDRESS_DETAIL_PID,
                A.GEOG.STX,
                A.GEOG.STY,
                A.STREET_NAME,
                A.STREET_TYPE_CODE,
                A.STREET_SUFFIX_CODE,
                AD.LOCALITY_NAME
            FROM
            ADDRESS_GNAF A
            INNER JOIN ADDRESS_GNAF_DETAIL AD
            ON A.ADDRESS_DETAIL_PID = AD.ADDRESS_DETAIL_PID
            ORDER BY ADDRESS_DETAIL_PID
            ''') as rows:
            for enum_address, row in enumerate(rows):
##        with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ADDRESS_GNAF'),
##                                   field_names=['ADDRESS_DETAIL_PID',
##                                                'SHAPE@X',
##                                                'SHAPE@Y',
##                                                'STREET_NAME',
##                                                'STREET_TYPE_CODE',
##                                                'STREET_SUFFIX_CODE',
####                                                'ROAD_NAME',
####                                                'ROAD_TYPE',
####                                                'ROAD_SUFFIX',
####                                                'LOCALITY_NAME',
##                                                'STATE_ABBREVIATION'],
##                                   where_clause=where_clause if where_clause else None,
##                                   sql_clause=(None, 'ORDER BY ADDRESS_DETAIL_PID')) as sc:
##            for enum_address, row in enumerate(sc):

                pfi, x, y, road_name, road_type, road_suffix, locality_name = row

                # validate address values
                road_name, road_type, road_suffix, route_flag = em.parse_roadname(road_name, road_type, road_suffix, 0)    
                
                soundex = gis.generate_soundex(road_name)
                rnid = em.check_roadname(road_name, road_type, road_suffix)[1]
                yield pfi, x, y, road_name, road_type, road_suffix, rnid, soundex, locality_name

    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'ADDRESS_GNAF_ROAD_VALIDATION') as sbc, \
         arcpy.da.InsertCursor(in_table=temp_arv_fc,
                               field_names=['ADDR_PFI',
                                            'ROAD_PFI',
                                            'RULE_RANK',
                                            'RULE_CODE',
                                            'RULE_SCORE',
                                            'VALID_ATTR',
                                            'VALID_SPATIAL',
                                            'ADDR_RNID',
                                            'ROAD_RNID',
                                            'ADDR_LOCALITY_NAME',
                                            'ROAD_LOCALITY_NAME',
                                            'ADDR_SOUNDEX',
                                            'ROAD_SOUNDEX',
                                            'INTERSECTS',
                                            'SIDE_OF_ROAD',
                                            'DIST_FROM_ROAD',
                                            'DIST_ALONG_ROAD',
                                            'Shape@WKT']) as ic:
        work_gen = work_generator(estamap_version)
        num_work = 0
        num_results = 0
        total_results = 0
        work_gen_complete = False
        while True:

            try:
                work = next(work_gen)
                socket_work.send_pyobj(work)
                num_work = num_work + 1
            except StopIteration:
                work_gen_complete = True

            socks = dict(poller.poll(10))
          
            if socks.get(socket_result) == zmq.POLLIN:
                work_result = socket_result.recv_pyobj()
                num_results = num_results + 1

                pfi, results = work_result
                for result in results:
                    
                    ic.insertRow([pfi,] + result)
##                    sbc_all.add_row([pfi,] + result[:-1] + [geom,])
                    total_results = total_results + 1

                geom = sql_geom.Parse(clr.System.Data.SqlTypes.SqlString(result[-1]))
                geom.set_STSrid(clr.System.Data.SqlTypes.SqlInt32(3111))    
                sbc.add_row([pfi,] + result[:-1] + [geom,])

                if num_results % 10000 == 0:
                    logging.info('{}'.format((num_results, total_results)))
                    sbc.flush()
##                    sbc_all.flush()
                if work_gen_complete and num_work == num_results:
                    logging.info('{}'.format((num_results, total_results)))
                    sbc.flush()
##                    sbc_all.flush()
                    break

        socket_cmd.send('finish')
        # close processes
        for p in processes:
            p.join(1)
            p.terminate()


def validate_address_gnaf(estamap_version, where_clause=None):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    v = Validator(estamap_version, 'ADDRESS_ROAD')
    temp_fgdb = 'c:\\temp\\address_gnaf_validation_{}.gdb'.format(estamap_version)
    temp_arv_fc = os.path.join(temp_fgdb, 'ADDRESS_GNAF_ROAD_VALIDATION')

    logging.info('clr')
    clr.AddReference('System')
    clr.AddReference('System.Data')
    clr_sqlserver_path = r'C:\Program Files (x86)\Microsoft SQL Server\130\SDK\Assemblies'
    if clr_sqlserver_path not in sys.path:
        sys.path.append(clr_sqlserver_path)
    clr.AddReference('Microsoft.SqlServer.Types')
    sql_geom = clr.Microsoft.SqlServer.Types.SqlGeometry()

    logging.info('creating validation fc')
    dbpy.exec_script(em.server, em.database_name, os.path.join(em.path, 'SQL', 'validation', 'create_address_gnaf_road_validation.sql'))

    logging.info('creating temp fgdb')
    if arcpy.Exists(temp_fgdb):
        arcpy.Delete_management(temp_fgdb)
    arcpy.CreateFileGDB_management(*os.path.split(temp_fgdb))

    logging.info('creating temp validation fc')
    arcpy.CreateFeatureclass_management(out_path=temp_fgdb,
                                        out_name='ADDRESS_GNAF_ROAD_VALIDATION',
                                        geometry_type='POLYLINE',
                                        template=os.path.join(em.sde, 'ADDRESS_GNAF_ROAD_VALIDATION'),
                                        spatial_reference=arcpy.SpatialReference(3111))
    

    logging.info('looping address_gnaf')
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ADDRESS_GNAF'),
                               field_names=['ADDRESS_DETAIL_PID',
                                            'SHAPE@X',
                                            'SHAPE@Y',
                                            'STREET_NAME',
                                            'STREET_TYPE_CODE',
                                            'STREET_SUFFIX_CODE',
##                                            'ROAD_NAME',
##                                            'ROAD_TYPE',
##                                            'ROAD_SUFFIX',
                                            'LOCALITY_NAME'],
                               where_clause=where_clause if where_clause else None,
                               sql_clause=(None, 'ORDER BY ADDRESS_DETAIL_PID')) as sc, \
         dbpy.SQL_BULK_COPY(em.server, em.database_name, 'ADDRESS_GNAF_ROAD_VALIDATION') as sbc, \
         arcpy.da.InsertCursor(in_table=temp_arv_fc,
                               field_names=['ADDR_PFI',
                                            'ROAD_PFI',
                                            'RULE_RANK',
                                            'RULE_CODE',
                                            'RULE_SCORE',
                                            'VALID_ATTR',
                                            'VALID_SPATIAL',
                                            'ADDR_RNID',
                                            'ROAD_RNID',
                                            'ADDR_LOCALITY_NAME',
                                            'ROAD_LOCALITY_NAME',
                                            'ADDR_SOUNDEX',
                                            'ROAD_SOUNDEX',
                                            'INTERSECTS',
                                            'SIDE_OF_ROAD',
                                            'DIST_FROM_ROAD',
                                            'DIST_ALONG_ROAD',
                                            'Shape@WKT']) as ic:
        for enum_address, row in enumerate(sc):
            pfi, x, y, road_name, road_type, road_suffix, locality_name = row

            # validate address values
            road_name, road_type, road_suffix, route_flag = em.parse_roadname(road_name, road_type, road_suffix, 0)
            
            soundex = gis.generate_soundex(road_name)
            rnid = em.check_roadname(road_name, road_type, road_suffix)[1]
            
            results = v.validate(x, y, road_name, road_type, road_suffix, rnid, soundex, locality_name)

            # insert individual results into temp fc
            for result in results:
                ic.insertRow([pfi,] + result)

            geom = sql_geom.Parse(clr.System.Data.SqlTypes.SqlString(result[-1]))
            geom.set_STSrid(clr.System.Data.SqlTypes.SqlInt32(3111))    
            sbc.add_row([pfi,] + result[:-1] + [geom,])

            if enum_address % 1000 == 0:
                logging.info(enum_address)
                sbc.flush()
        logging.info(enum_address)
        sbc.flush()


if __name__ == '__main__':

    sys.argv.append('--estamap_version=DEV')

    with log.LogConsole():
        
        logging.info('parsing args')
        args = docopt(__doc__)

        logging.info('variables')
        estamap_version = args['--estamap_version']
        where_clause = args['--where_clause']
        log_file = args['--log_file']
        log_path = args['--log_path']

        with log.LogFile(log_file, log_path):
            logging.info('start')
            try:
                ###########

##                em = gis.ESTAMAP(estamap_version)
##                v = Validator(estamap_version, 'ADDRESS_ROAD', rebuild=False)
##
##                logging.info('looping address')
##                with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ADDRESS'),
##                                           field_names=['PFI',
##                                                        'SHAPE@X',
##                                                        'SHAPE@Y',
##                                                        'ROAD_NAME',
##                                                        'ROAD_TYPE',
##                                                        'ROAD_SUFFIX',
##                                                        'LOCALITY_NAME'],
##                                           where_clause='PFI = 125498789',
##                                           sql_clause=(None, 'ORDER BY PFI')) as sc:
##                    for enum_address, row in enumerate(sc):
##                        pfi, x, y, road_name, road_type, road_suffix, locality_name = row
##
##                        # validate address values
##                        road_name, road_type, road_suffix, route_flag = em.parse_roadname(road_name, road_type, road_suffix, 0)
##                        
##                        soundex = gis.generate_soundex(road_name)
##                        rnid = em.check_roadname(road_name, road_type, road_suffix)[1]
##                        
##                        results = v.validate(x, y, road_name, road_type, road_suffix, rnid, soundex, locality_name)
##                        for result in results:
##                            print result
##                        if enum_address % 1000 == 0:
##                            logging.info(enum_address)

                ###########
                
##                validate_address_mp(estamap_version, where_clause)
                validate_address_gnaf_mp(estamap_version, where_clause)
                

                ###########   
            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')

 
