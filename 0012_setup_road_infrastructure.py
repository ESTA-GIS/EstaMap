'''
Intersection setup.

Register new road infrastructure.


Usage:
  setup_road_infrastructure.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  --log_file <file>       Log File name. [default: 012setup_road_infrastructure.log]
  --log_path <folder>     Folder to store the log file. [default: C:\\Temp]
'''
import time
import os
import logging

import log
from docopt import docopt
import dev as gis

import arcpy


def register_new_roadinfrastructure(estamap_version):
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    cursor = em.conn.cursor()

    sql = '''
INSERT INTO ROAD_INFRASTRUCTURE_MSLINK_REGISTER
(UFI, SOURCE_DATASET, SOURCE_PK)
SELECT
    DISTINCT UFI,
    'TR_ROAD_INFRA.{vm}'  AS SOURCE_DATASET,
    UFI AS SOURCE_PK
FROM ROAD_INFRASTRUCTURE 
WHERE
    UFI NOT IN (SELECT UFI FROM DBO.ROAD_INFRASTRUCTURE_MSLINK_REGISTER)
ORDER BY ROAD_INFRASTRUCTURE.UFI
'''.format(vm=em.vicmap_version)
    results = cursor.execute(sql)
    num_inserted = results.rowcount
    logging.info('Road Infrastructure registered: {}'.format(num_inserted))
    

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

                register_new_roadinfrastructure(estamap_version)
                
            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')
