'''
Create vicmap database and sde connection file.
  Database name created from VicMap folder name.
  SDE Connection file created in the VicMap folder.

  Sets:
	the DB size = 15gb.
	recovery model = simple

  Grants read access to:
	ESTAMIS
	ArcGISSOC

  Run this script using Python 32bit, had problem with 64bit.

Usage:
  create_sde_db.py [options]

Options:
  --vicmap_version <version>  VicMap Version
  --auth_file <file>        ArcServer Authorization File
  --log_file <file>         Log File name. [default: 002create_sde_db.log]
  --log_path <folder>       Folder to store the log file. [default: c:\\temp\\EM19]
'''
import time
import os
import logging
import sys

import log
from docopt import docopt
import arcpy

import dev as gis

def create_VMsde_db(vicmap_version, auth_file):

    logging.info('checking python interpreter is 32 bit')
    if sys.maxsize > 2**32:
        raise Exception('Please use Python 32 bit.')

    logging.info('local variables')
    vm = gis.VICMAP(vicmap_version)
    
    logging.info('creating SDE geodatabase: ' + vm.database_name)
    arcpy.CreateEnterpriseGeodatabase_management(database_platform="SQL_Server",
                                                 instance_name="TDB03",
                                                 database_name=vm.database_name,
                                                 account_authentication="OPERATING_SYSTEM_AUTH",
                                                 database_admin="#",
                                                 database_admin_password="#",
                                                 sde_schema="DBO_SCHEMA",
                                                 gdb_admin_name="#",
                                                 gdb_admin_password="#",
                                                 tablespace_name="#",
                                                 authorization_file=auth_file)


    logging.info('create database connection')
    conn = arcpy.ArcSDESQLExecute('TDB03', 'sde:sqlserver:TDB03')


    logging.info('grant permissions to users')
    sql = '''
            USE [{vm}];
            CREATE USER [ESTA\ArcGISSOC] FOR LOGIN [ESTA\ArcGISSOC];
            ALTER ROLE [db_datareader] ADD MEMBER [ESTA\ArcGISSOC];
            CREATE USER [ESTA\ESTAMIS] FOR LOGIN [ESTA\ESTAMIS];
            ALTER ROLE [db_datareader] ADD MEMBER [ESTA\ESTAMIS];
            '''.format(vm=vm.database_name)
    logging.info('sql: ' + sql)
    result = conn.execute(sql)
    logging.info('result: {result}'.format(result=result))


    logging.info('get server property info')
    logical_name_db = conn.execute('''select name from sys.master_files where database_id = db_id('{vm}') and type_desc = 'ROWS' '''.format(vm=vm.database_name))
    logical_name_log = conn.execute('''select name from sys.master_files where database_id = db_id('{vm}') and type_desc = 'LOG' '''.format(vm=vm.database_name))
    size_db = conn.execute('''select (size*8)/1024 as size from sys.master_files where database_id = db_id('{vm}') and type_desc = 'ROWS' '''.format(vm=vm.database_name))

    if size_db != 15360:
        logging.info('alter database size: 15gb')
        sql = '''
        COMMIT;
        ALTER DATABASE [{vm}]
                MODIFY FILE (
                        NAME = '{name_db}',
                        SIZE = 30GB,
                        MAXSIZE = UNLIMITED,
                        FILEGROWTH = 10%
                );
        BEGIN TRANSACTION;
        '''.format(vm=vm.database_name,
                   name_db=logical_name_db,
                   name_log=logical_name_log)
        logging.info('sql: ' + sql)
        result = conn.execute(sql)
        logging.info('result: {result}'.format(result=result))
    

    logging.info('alter database recovery')
    sql = '''
            COMMIT;
            ALTER DATABASE [{vm}]
                    SET RECOVERY SIMPLE WITH NO_WAIT;
            BEGIN TRANSACTION;
            '''.format(vm=vm.database_name)
    logging.info('sql: ' + sql)
    result = conn.execute(sql)
    logging.info('result: {result}'.format(result=result))


    logging.info('creating SDE connection file: ' + vm.sde)
    if not os.path.exists(vm.sde):
        arcpy.CreateArcSDEConnectionFile_management(out_folder_path=os.path.split(vm.sde)[0],
                                                    out_name=os.path.split(vm.sde)[1],
                                                    server='TDB03',
                                                    service='sde:sqlserver:TDB03',
                                                    database=vm.database_name,
                                                    account_authentication="OPERATING_SYSTEM_AUTH",
                                                    version="dbo.DEFAULT",
                                                    save_version_info="SAVE_VERSION",)

if __name__ == '__main__':

    sys.argv.append('--vicmap_version=20180524')
    sys.argv.append('--auth_file=M:\\development\\ESRI\\Authorisation\\10.2\\ArcGISforServerAdvancedEnterprise_server_209415_Full.ecp')
    
    with log.LogConsole(level='WARNING'):
            
        logging.info('parsing args')
        args = docopt(__doc__)

        logging.info('variables')
        vicmap_version = args['--vicmap_version']
        auth_file = args['--auth_file']
        log_file = args['--log_file']
        log_path = args['--log_path']

        with log.LogFile(log_file, log_path):
            logging.info('start')
            try:
                create_VMsde_db(vicmap_version, auth_file)
            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')

