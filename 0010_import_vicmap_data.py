'''
Imports feature classes and tables from VicMap database into ESTAMAP database.

Usage:
  import_vicmap_data.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  --vicmap_version <version>  VicMap Version
  --gnaf_version <version>  GNAF Version
  --log_file <file>       Log File name. [default: 10import_vicmap_data.log]
  --log_path <folder>     Folder to store the log file. [default: C:\\Temp\EM19]
'''
import time
import os
import logging

from docopt import docopt
import arcpy
import shapely.geometry
import shapely.wkb
import shapely.prepared
import rtree

import dev as gis
import log




##import_list = [
##    # Admin
##    'AD_LOCALITY_AREA_POLYGON',
##    'AD_LGA_AREA_POLYGON',
##    
##    # Address
##    'ADDRESS',
##    
##    # Transport
##    'TR_ROAD',
##    'TR_ROAD_INFRASTRUCTURE',
##    
##    # Reference
##    '',
##    ]

import_list = {}

# Admin
import_list['AD_LOCALITY_AREA_POLYGON'] = 'LOCALITY'
import_list['AD_LGA_AREA_POLYGON'] = 'LGA'

# Address
import_list['ADDRESS'] = 'ADDRESS'

# Transport
import_list['TR_ROAD'] = 'ROAD'
import_list['TR_ROAD_INFRASTRUCTURE'] = 'ROAD_INFRASTRUCTURE'

# Reference

# VMLITE
import_list['VMLITE_VICTORIA_POLYGON_SU5'] = 'VICTORIA_POLYGON'


def import_vicmap_data(estamap_version, vicmap_version):
    vm = gis.VICMAP(vicmap_version)
    em = gis.ESTAMAP(estamap_version)

    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(3111)

    import_features = []

    for vm_fc, em_fc in import_list.iteritems():

##        if arcpy.Exists(os.path.join(em.sde, em_fc)):
##            print '4'
##            vm_count = arcpy.GetCount_management(os.path.join(vm.sde, vm_fc)).getOutput(0)
##            em_count = arcpy.GetCount_management(os.path.join(em.sde, em_fc)).getOutput(0)
##            if vm_count == em_count:
##                continue
##            arcpy.Delete_management(os.path.join(em.sde, em_fc))

        logging.info('importing: {}'.format(os.path.join(em.sde, em_fc)))
        arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(vm.sde, vm_fc),
                                                    out_path=em.sde,
                                                    out_name=em_fc)

##    logging.info('walking vicmap database')
##    for base_path, dirs, fcs in arcpy.da.Walk(vm.sde, datatype=['FeatureClass', 'Table']):
##        for f in fcs:
##
##            db, dbo, f_name = arcpy.ParseTableName(f, vm.sde).split(', ')
##            if f_name.upper() in import_list:
##                logging.info(f_name)
##                print arcpy.Exists(os.path.join(em.sde, f_name))
##                if arcpy.Exists(os.path.join(em.sde, f_name)):
##                    vm_count = arcpy.GetCount_management(os.path.join(vm.sde, f_name)).getOutput(0)
##                    em_count = arcpy.GetCount_management(os.path.join(em.sde, f_name)).getOutput(0)
##                    if vm_count == em_count:
##                        continue
##                    arcpy.Delete_management(os.path.join(em.sde, f_name))
##                print os.path.join(vm.sde, f_name)
##                import_features.append(os.path.join(vm.sde, f_name))
##    print import_features
##    if len(import_features):
##        logging.info('importing layers')
##        arcpy.FeatureClassToGeodatabase_conversion(Input_Features=import_features,
##                                                   Output_Geodatabase=em.sde)
    
##    for base_path, dirs, fcs in arcpy.da.Walk(vm.sde):
##
##        for enum, fc_db_name in enumerate(fcs, 1):
##            
##            db_name, db_schema, fc_name = fc_db_name.split('.')
##            input_fc = os.path.join(vm.sde, fc_name)
##            output_fc = os.path.join(em.sde, fc_name)
##
##            if db_schema.lower() <> 'dbo':
##                logging.info('skipping: ' + fc_db_name)
##                continue
##            
##            if arcpy.Exists(output_fc):
##                logging.info('input exists skipping: ' + output_fc)
##            else:
##                logging.info('copying: ' + fc_db_name)
##                arcpy.Copy_management(in_data=input_fc,
##                                      out_data=output_fc)
##
##            logging.info('output ({e}/{total}): {fc}'.format(e=enum, total=len(fcs), fc=output_fc))




if __name__ == '__main__':

    sys.argv.append('--estamap_version=19')
    sys.argv.append('--vicmap_version=20180524')
    sys.argv.append('--gnaf_version=201805')
        
    with log.LogConsole():
        
        logging.info('parsing args')
        args = docopt(__doc__)

        logging.info('variables')
        estamap_version = args['--estamap_version']
        vicmap_version = args['--vicmap_version']
        gnaf_version = args['--gnaf_version']
        log_file = args['--log_file']
        log_path = args['--log_path']

        with log.LogFile(log_file, log_path):
            logging.info('start')
            try:
               import_vicmap_data(estamap_version, vicmap_version)
                
            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')
