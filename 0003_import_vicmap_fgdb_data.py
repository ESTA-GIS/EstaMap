'''
Creates batch file to import VicMap data.

notes:
may need to add a overwrite check on data imported already.

Usage:
  import_vicmap_data.py [options]

Options:
  --vicmap_version <version>    VicMap Version
  --log_file <file>         Log File name. [default: 003import_vicmap_data.log]
  --log_path <folder>       Folder to store the log file. [default: C:\\Temp\\EM19]
'''
import sys
import os
import time
import logging
import subprocess

from docopt import docopt
import arcpy

import log
import dev as gis

def import_vicmap_fgdb_data(vicmap_version):

    logging.info('local variables')
    vm = gis.VICMAP(vicmap_version)

    imported_fcs = []
    imported_tables = []

    logging.info('exclusion list:')
    exclude_list = [
                    # VMELEV
                    'EL_CONTOUR_1TO5M', 
                    'EL_GRND_SURFACE_POINT_1TO5M',
                    # VMPROP
                    'ANNOTATION_TEXT', 
                    'CAD_AREA_BDY',
                    'EASEMENT',
                    'CENTROID',
                    'PARCEL_CAD_AREA_BDY',
                    'PROPERTY_CAD_AREA_BDY',
                    ]
    for exclude in exclude_list:
        logging.info(exclude)

    logging.info('starting import')
    for root, gdb_names, files in arcpy.da.Walk(vm.path, datatype='Container'):

        for gdb_name in gdb_names:

            if gdb_name.lower().endswith('gdb'):
                logging.info(gdb_name)

                gdb_folder = os.path.join(vm.path, gdb_name)
                arcpy.env.workspace = gdb_folder
                
                fcs = []
                fc_names = arcpy.ListFeatureClasses()
                for fc in fc_names:
                    if fc.upper() in exclude_list:
                        continue
                    if arcpy.Exists(os.path.join(vm.path, vm.sde, fc)):
                        logging.info('exists: {}'.format(fc))
                        continue
                    logging.info('loading feature class: ' + fc)
                    fcs.append(os.path.join(gdb_folder, fc))
                if fcs:
                    arcpy.FeatureClassToGeodatabase_conversion(Input_Features=fcs,
                                                               Output_Geodatabase=os.path.join(vm.path, vm.sde))
                    imported_fcs.extend(fcs)

                tables = []
                table_names = arcpy.ListTables()
                for table in table_names:
                    if table.upper() in exclude_list:
                        continue
                    if arcpy.Exists(os.path.join(vm.path, vm.sde, table)):
                        logging.info('exists: {}'.format(table))
                        continue
                    logging.info('loading table: ' + table)
                    tables.append(os.path.join(gdb_folder, table))
                if tables:
                    arcpy.TableToGeodatabase_conversion(Input_Table=tables,
                                                        Output_Geodatabase=os.path.join(vm.path, vm.sde))
                    imported_tables.extend(tables)
    return [imported_fcs, imported_tables]

def report_vicmap_count(vm, output_report):

    with gis.XLSX(output_report) as report:
        counts_ws = report.add_worksheet('VicMap Counts')
        counts_ws.set_column(0,0,60.0)        
        report.append_row(worksheet_name='VicMap Counts',
                          row_data=['Layer Name', 'Count'],
                          format_name='header')

        conn = arcpy.ArcSDESQLExecute(vm.sde)
        for dirpath, dirnames, files in arcpy.da.Walk(workspace=vm.sde,
                                                      followlinks=True,
                                                      datatype=['Table', 'FeatureClass']):
            for f in sorted(files):
                sql = 'select count(*) from {table}'.format(table=f)
                count = conn.execute(sql)
                logging.info('{f}: {c}'.format(f=f, c=count))
                report.append_row(worksheet_name='VicMap Counts',
                                  row_data=[f, count])
                
    return output_report


if __name__ == '__main__':

    sys.argv.append('--vicmap_version=20180524')
    
    with log.LogConsole(level='WARNING'):

        logging.info('parsing args')
        args = docopt(__doc__)

        logging.info('variables')
        vicmap_version = args['--vicmap_version']
        log_file = args['--log_file']
        log_path = args['--log_path']
        
        with log.LogFile(log_file, log_path):
            logging.info('start')
            try:
                
                # import data
                fcs, tables = import_vicmap_fgdb_data(vicmap_version)
                logging.info('Total Feature Classes: {}'.format(len(fcs)))
                logging.info('Total Tables: {}'.format(len(tables)))

##                # generate report
##                output_report = os.path.join(vm.folder, vm.db_name + '_counts.xlsx')
##                vm_report = report_vicmap_count(vm, output_report)
##                logging.info('VM Report: {}'.format(vm_report))

            except:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')
