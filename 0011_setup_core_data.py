'''
Imports feature classes and tables from VicMap database into ESTAMAP database.

Usage:
  setup_core_tables.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  --log_file <file>       Log File name. [default: 011Setup_CoreData.log]
  --log_path <folder>     Folder to store the log file. [default: C:\\Temp\EM19]
'''
import time
import os
import logging

from docopt import docopt
import arcpy

import dev as gis
import log
import dbpy



core_tables = [
        'ADDRESS_MSLINK_REGISTER',
        'ADDRESS_TEMP_MSLINK_REGISTER',
        'COPL_MSLINK_REGISTER',
        'LOCALITY_MSLINK_REGISTER',
        'ROAD_INFRASTRUCTURE_MSLINK_REGISTER',
        'ROAD_NAME_REGISTER',
        'ROAD_TYPE_REGISTER',

        'VALIDATION_RULE',
        'VALIDATION_CATEGORY',
        'VALIDATION_CATEGORY_RULE',
        ]

def create_core_tables(estamap_version):

    em = gis.ESTAMAP(estamap_version)
    for core_table in core_tables:
        logging.info('creating core table: {}'.format(core_table))
        sql_script = os.path.join(em.path, 'sql', 'core_tables', 'create_' + core_table + '.sql')
        logging.info('running sql script: {}'.format(sql_script))

        dbpy.exec_script(em.server, em.database_name, sql_script)

def import_core_data(estamap_version):

    em = gis.ESTAMAP(estamap_version)
    for core_table in core_tables:
        logging.info('importing core table data: {}'.format(core_table))
        sql_script = os.path.join(em.path, 'sql', 'core_tables', 'import_' + core_table + '.sql')
        logging.info('running sql script: {}'.format(sql_script))

        dbpy.exec_script(em.server, em.database_name, sql_script)

if __name__ == '__main__':

    sys.argv.append('--estamap_version=19')
        
        
    with log.LogConsole(level='WARNING'):
        logging.info('parsing args')
        args = docopt(__doc__)

        logging.info('variables')
        estamap_version = args['--estamap_version']
        log_file = args['--log_file']
        log_path = args['--log_path']

        with log.LogFile(log_file, log_path):
            logging.info('start')
            try:
                create_core_tables(estamap_version)
                import_core_data(estamap_version)
            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')
