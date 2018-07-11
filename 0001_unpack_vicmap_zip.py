'''
Python Script to unpack VicMap zipfiles.

# http://stackoverflow.com/questions/16094229/python-unzip-a-file-to-current-working-directory-but-not-maintain-directory-str

Usage:
  unpack_vicmap_zip.py [options]

Options:
  --vicmap_version <version>  VicMap Version
  --log_file <file>         Log File name. [default: 001_unpack_vicmap_zip.log]
  --log_path <folder>       Folder to store the log file. [default: c:\\temp\\EM19]
'''
import zipfile
import glob
import os
import time
import sys
import logging

from docopt import docopt
import log

import dev as gis

def unpack_vicmap_zip(vicmap_version):

    vm = gis.VICMAP(vicmap_version)


    logging.info('include file list...')
    include_list = [
                    'VICMAP_ADDRESS.GDB.ZIP'
                    ,'VICMAP_INDEX.GDB.ZIP'
                    ,'VMADMIN.GDB.ZIP'
                    ,'VMCLTENURE.GDB.ZIP'
                    ,'VMFOI.GDB.ZIP'
                    ,'VMHYDRO.GDB.ZIP'
                    ,'VMLITE.GDB.ZIP'
                    ,'VMPLAN.GDB.ZIP'
                    ,'VMPROP.GDB.ZIP'
                    ,'VMPROP_SIMPLIFIED_2.GDB.ZIP'
                    ,'VMREFTAB.GDB.ZIP'
                    ,'VMTRANS.GDB.ZIP'
                    ,'VMVEG.GDB.ZIP'
                   ]
    extracted_list = []
    # for f in exclude_list:
    #     logging.info(f)

    logging.info('search folder: ' + os.path.join(vm.path, '*.zip'))
    logging.info('unpacking...')

    for f in glob.glob(os.path.join(vm.path, '*.zip')):
        
        if os.path.basename(f).upper() not in include_list:
            continue

        extracted_list.append(os.path.basename(f).upper())
        logging.info(os.path.basename(f).upper())
        path, f_file = os.path.split(f)
        f_name, f_ext = os.path.splitext(f)
        extraction_path = os.path.abspath(os.path.join(path, f_name))

        if not os.path.exists(extraction_path):
            logging.info('creating extraction path :'+ extraction_path)
            os.makedirs(extraction_path)

        logging.info('extraction path: ' + extraction_path)
        with zipfile.ZipFile(f, 'r') as zipf:

            for info in zipf.infolist():
                fn, dtz = info.filename, info.date_time

                logging.info('extracting: ' + fn)

                name = os.path.basename(fn)
                if not name:
                    continue

                c = zipf.open(fn)
                outfile = os.path.join(extraction_path, fn)

                chunk = 2**16
                try:
                    with open(outfile, 'wb') as f:
                        s = c.read(chunk)
                        f.write(s)
                        while not len(s) < chunk:
                            s = c.read(chunk)
                            f.write(s)
                    c.close()
                    dtout = time.mktime(dtz + (0, 0, -1))
                    os.utime(outfile, (dtout, dtout))
                except IOError:
                    logging.exception('exc occured')
                    c.close()
            logging.info('complete.')

    if extracted_list == include_list:
        print ("All GDB files exist and extracted successfully")
    else:
        print("Some GDB files not found/extracted :", set[include_list]-set[extracted_list])
        logging.warning("GDB files not found/extracted", set[include_list]-set[extracted_list])
##            # loop and extract files in zip file
##            for sub_file in zipf.namelist():
##
##                if sub_file in exclude_list:
##                    logging.info('skipping: ' + sub_file)
##                    continue
##
##                logging.info('extracting: ' + sub_file)
##                zipf.extract(sub_file, extraction_path)


if __name__ == '__main__':

    sys.argv.append('--vicmap_version=20180524')

    with log.LogConsole(level='WARNING'):

        logging.info('parsing arguments')
        args = docopt(__doc__)

        logging.info('variables')
        vicmap_version = args['--vicmap_version']
        log_file = args['--log_file']
        log_path = args['--log_path']
        
        with log.LogFile(log_file, log_path):
            logging.info('start')
            try:
                unpack_vicmap_zip(vicmap_version)
            except:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')
