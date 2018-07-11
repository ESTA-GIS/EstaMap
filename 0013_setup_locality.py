'''
Locality Setup.

Usage:
  setup_locality.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  --log_file <file>       Log File name. [default: setup_locality.log]
  --log_path <folder>     Folder to store the log file. [default: c:\\temp]
'''
import time
import os
import logging

import log
from docopt import docopt
import dev as gis

import arcpy


def setup_locality(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    arcpy.MakeFeatureLayer_management(in_features=os.path.join(em.sde, 'LOCALITY'),
                                      out_layer='in_memory\\locality_layer')
    
    arcpy.SelectLayerByAttribute_management(in_layer_or_view='in_memory\\locality_layer',
                                            selection_type='NEW_SELECTION',
                                            where_clause="NAME = 'VIC'")

    if int(arcpy.GetCount_management('in_memory\\locality_layer').getOutput(0)) == 1:
        logging.info('removing VIC polygon')
        arcpy.DeleteFeatures_management('in_memory\\locality_layer')


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

                setup_locality(estamap_version)                
                
            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')
