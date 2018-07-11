'''
Post ADDRESS Validation against ROAD.

MODIFY TO USE ADDRESS_COMPONENTS TABLE.

Usage:
  address_post_validation.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  --log_file <file>       Log File name. [default: address_validation.log]
  --log_path <folder>     Folder to store the log file. [default: c:\\temp]
'''
import os
import sys
import logging
import shutil

from docopt import docopt
import pandas as pd
import arcpy
import lmdb

import log
import dev as gis
import dbpy


def address_validation_phase_1(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_sqlalchemy(em.server, em.database_name, init_geomtype='clr')

    logging.info('dropping tables:')
    if dbpy.check_exists('ADDRESS_DUPLICATE_RESOLUTION_PHASE_1', conn):
        logging.info('ADDRESS_DUPLICATE_RESOLUTION_PHASE_1')
        conn.execute('drop table ADDRESS_DUPLICATE_RESOLUTION_PHASE_1')
    if dbpy.check_exists('ADDRESS_VALIDATION', conn):
        logging.info('ADDRESS_VALIDATION')
        conn.execute('drop table ADDRESS_VALIDATION')
    if dbpy.check_exists('ADDRESS_EXCLUSION', conn):
        logging.info('ADDRESS_EXCLUSION')
        conn.execute('drop table ADDRESS_EXCLUSION')
    if dbpy.check_exists('ADDRESS_VALIDATED_PHASE_1', conn):
        logging.info('ADDRESS_VALIDATED_PHASE_1')
        conn.execute('drop table ADDRESS_VALIDATED_PHASE_1')
    if dbpy.check_exists('ADDRESS_DUPLICATE_RESOLUTION_PHASE_1_SUMMARY', conn):
        logging.info('ADDRESS_DUPLICATE_RESOLUTION_PHASE_1_SUMMARY')
        conn.execute('drop table ADDRESS_DUPLICATE_RESOLUTION_PHASE_1_SUMMARY')
    if dbpy.check_exists('ADDRESS_DUPLICATE_RESOLUTION_PHASE_1_SUMMARY_UNIQUE', conn):
        logging.info('ADDRESS_DUPLICATE_RESOLUTION_PHASE_1_SUMMARY_UNIQUE')
        conn.execute('drop table ADDRESS_DUPLICATE_RESOLUTION_PHASE_1_SUMMARY_UNIQUE')
    
    logging.info('create ADDRESS_EXCLUSION')
    conn.execute('''
    CREATE TABLE [dbo].[ADDRESS_EXCLUSION](
        [PFI] [nvarchar](10) NOT NULL,
        [RULE_CODE] [nvarchar](20) NULL,
        [RULE_DESC] [nvarchar](255) NULL
    ) ON [PRIMARY]
    ''')

    # ----------
    logging.info('address exclusion')
    # from sql script: 300_AddressDuplicationPatches.sql
    conn.execute('''INSERT INTO ADDRESS_EXCLUSION (PFI, RULE_CODE, RULE_DESC) VALUES (220274547, 'REFERENCE', 'INC 38353'); ''')



    # ----------
    logging.info('finding excludes')
    
    # rule: ADDRESS_CLASS = M
    num_results = conn.execute('''
    INSERT INTO ADDRESS_EXCLUSION (PFI, RULE_CODE, RULE_DESC)
    SELECT PFI, 'CLASS_M', 'ADDRESS CLASS IS M'
    FROM ADDRESS
    WHERE ADDRESS_CLASS = 'M'
    ''')
    logging.info('CLASS_M: {}'.format(num_results.rowcount))

    
    # rule: ADDRESS ROAD SCORE < 50
    num_results = conn.execute('''
    INSERT INTO ADDRESS_EXCLUSION (PFI, RULE_CODE, RULE_DESC)
    SELECT ADDR_PFI, 'AR_L50', 'ADDRESS ROAD SCORE IS LESS THAN 50'
    FROM ADDRESS_ROAD_VALIDATION
    WHERE RULE_SCORE < 50
    ''')
    logging.info('AR_L50: {}'.format(num_results.rowcount))

##
##    # rule: PROPERTY_STATUS = P
##    num_results = conn.execute('''
##    INSERT INTO ADDRESS_EXCLUSION (PFI, RULE_CODE, RULE_DESC)
##    SELECT PFI, 'PROPOSED', 'PROPERTY_STATUS IS PROPOSED'
##    FROM ADDRESS
##    WHERE PROPERTY_STATUS = 'P'
##    ''')
##    logging.info('PROPOSED: {}'.format(num_results.rowcount))


    # rule: ADDRESS_CLASS = S non-conformance
    num_results = conn.execute('''
    INSERT INTO ADDRESS_EXCLUSION (PFI, RULE_CODE, RULE_DESC)
    SELECT PFI, 'NO_NUM', 'ADDRESS CLASS S HAS NO NUM'
    FROM ADDRESS
    WHERE HOUSE_NUMBER_1 IS NULL and ADDRESS_CLASS = 'S'
    ''')
    logging.info('NO_NUM: {}'.format(num_results.rowcount))

    # rule: DOES NOT HAVE PROPERTY PFI
    num_results = conn.execute('''
    INSERT INTO ADDRESS_EXCLUSION (PFI, RULE_CODE, RULE_DESC)
    SELECT A.PFI, 'NO_PROPPFI', 'ADDRESS HAS NO MATCHING PROPERTY PFI'
    FROM ADDRESS A
    LEFT JOIN PROPERTY P
    ON A.PROPERTY_PFI = P.PFI
    WHERE P.VIEW_PFI IS NULL
    ''')
    logging.info('NO_PROPPFI: {}'.format(num_results.rowcount))

    # rule: HOUSE_PREFIX_1 IS NOT NUMERIC
    num_results = conn.execute('''
    INSERT INTO ADDRESS_EXCLUSION (PFI, RULE_CODE, RULE_DESC)
    SELECT PFI, 'PREFIX_NOT_NUM', 'HOUSE_PREFIX_1 IS NOT A NUMBER'
    FROM ADDRESS
    WHERE ISNUMERIC(HOUSE_PREFIX_1)=1
    ''')
    logging.info('PREFIX_NOT_NUM: {}'.format(num_results.rowcount))

    # rule: ISNULL(FEATURE_QUALITY_ID,'') <> 'PAPER_ROAD_ONLY'
    num_results = conn.execute('''
    INSERT INTO ADDRESS_EXCLUSION (PFI, RULE_CODE, RULE_DESC)
    SELECT PFI, 'PAPER', 'ADDRESS FEATURE_QUALITY IS PAPER_ROAD_ONLY'
    FROM ADDRESS
    WHERE FEATURE_QUALITY_ID = 'PAPER_ROAD_ONLY'
    ''')
    logging.info('PAPER_ROAD_ONLY: {}'.format(num_results.rowcount))
    
    # ----------

    logging.info('setup ADDRESS_VALIDATION')
    conn.execute('''
    SELECT
        A.PFI,
        A.HOUSE_PREFIX_1,
        A.HOUSE_NUMBER_1,
        A.HOUSE_SUFFIX_1,
        A.HOUSE_PREFIX_2,
        A.HOUSE_NUMBER_2,
        A.HOUSE_SUFFIX_2,
        ISNULL(A.HOUSE_PREFIX_1,'') + CAST(A.HOUSE_NUMBER_1 AS VARCHAR(11)) + ISNULL(HOUSE_SUFFIX_1,'') as ST_NUM,
        ISNULL(A.HOUSE_PREFIX_2,'') + CAST(A.HOUSE_NUMBER_2 AS VARCHAR(11)) + ISNULL(HOUSE_SUFFIX_2,'') as HI_NUM,
        RN.ROAD_NAME,
        RN.ROAD_TYPE,
        RN.ROAD_SUFFIX,
        A.LOCALITY_NAME,
        ISNULL(A.LOCALITY_NAME,'')  + '_' + ISNULL(RN.ROAD_NAME,'') + '_' + ISNULL(RN.ROAD_TYPE,'') + '_' + ISNULL(RN.ROAD_SUFFIX,'') + '_' +  ISNULL(A.HOUSE_PREFIX_1,'') + CAST(A.HOUSE_NUMBER_1 AS VARCHAR(11)) + ISNULL(HOUSE_SUFFIX_1,'') as ADDRESS_STRING,

        I.ROAD_NAME_ID,
        A.IS_PRIMARY,
        P.STATUS,
        CAST(ISNULL(CAST(A.BLG_UNIT_ID_1 AS VARCHAR)+ ISNULL(CAST(A.BLG_UNIT_SUFFIX_1 AS VARCHAR),''), A.FLOOR_NO_1) AS VARCHAR(5)) as LV_APT,
        PV.GRAPHIC_TYPE,
        AVR.RULE_SCORE,
        AVR.DIST_FROM_ROAD,
        AVR.ROAD_PFI,
        A.SHAPE
        
    INTO ADDRESS_VALIDATION

    FROM ADDRESS A

    -- exclusions
    LEFT JOIN ADDRESS_EXCLUSION B
        ON A.PFI = B.PFI

    -- ROAD_NAME_ID
    LEFT JOIN ADDRESS_RNID I
        ON A.PFI = I.PFI
    LEFT JOIN ROAD_NAME_REGISTER RN
        ON I.ROAD_NAME_ID = RN.ROAD_NAME_ID

    -- PROPERTY GRAPHIC_TYPE
    LEFT JOIN PROPERTY P
        ON A.PROPERTY_PFI = P.PFI
    LEFT JOIN PROPERTY_VIEW PV
        ON P.VIEW_PFI = PV.PFI

    -- ADDRESS_ROAD_VALIDATION RULE_SCORE
    LEFT JOIN ADDRESS_ROAD_VALIDATION AVR
        ON A.PFI = AVR.ADDR_PFI
    
    WHERE B.PFI IS NULL
    ''')

    logging.info('reading ADDRESS_VALIDATION')
    address_data = pd.read_sql('''SELECT * FROM ADDRESS_VALIDATION''', conn)
    logging.info('setting index on PFI')
    address_data.set_index('PFI', drop=False, inplace=True)
    

    # ----------
    logging.info('find duplicates')

    address_duplicates = address_data.duplicated(subset=[
        'ST_NUM',
        'ROAD_NAME',
        'ROAD_TYPE',
        'ROAD_SUFFIX',
        'LOCALITY_NAME'],
        keep=False)
    address_duplicates_pfis = address_duplicates[address_duplicates == True]
    logging.info('ADDRESS DUPLICATES: {}'.format(len(address_duplicates_pfis)))
    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'ADDRESS_EXCLUSION') as sbc_excl:
        sbc_excl.load_data(((pfi, 'DUPLICATE', 'ADDRESS HAS DUPLICATES') for pfi in address_duplicates_pfis.index.values))


    # ----------
    logging.info('create ADDRESS_DUPLICATE_RESOLUTION_PHASE_1')
    conn.execute('''
    CREATE TABLE [dbo].[ADDRESS_DUPLICATE_RESOLUTION_PHASE_1](
        [PFI] [nvarchar](10) NOT NULL,
        [RESOLUTION_PFI] [nvarchar](10) NULL,
        [RESOLUTION_CODE] [nvarchar](20) NULL,
        [RESOLUTION_DESC] [nvarchar](255) NULL,
        [ADDRESS_STRING] [nvarchar](255) NULL
    ) ON [PRIMARY]
    ''')

    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'ADDRESS_DUPLICATE_RESOLUTION_PHASE_1') as sbc_res:
        logging.info('resolving duplicates - phase 1')
        address_duplicates_data = address_data[address_duplicates]

        grouped = address_duplicates_data.groupby(by=[
            'ST_NUM',
            'ROAD_NAME',
            'ROAD_TYPE',
            'ROAD_SUFFIX',
            'LOCALITY_NAME'])

        temp = []
        logging.info('groups: {}'.format(len(grouped.groups)))
        for enum, (item, pfis) in enumerate(grouped.groups.iteritems()):

            # get address data
            data = address_duplicates_data.ix[pfis].sort_values(by=['IS_PRIMARY', 'RULE_SCORE', 'DIST_FROM_ROAD', 'HOUSE_NUMBER_2', 'LV_APT', 'PFI'],
                                                                ascending=[False, False, True, False, True, False])
            
            # rule #1: single IS_PRIMARY
            if len(data[data['IS_PRIMARY'] == 'Y']) == 1:
                resolution_pfi = data[data['IS_PRIMARY'] == 'Y'].index.values[0]
                resolution_code = 'IS_PRIMARY'
                resolution_desc = 'SINGLE IS_PRIMARY'

            # rule #2: single BASE_PROP
            elif len(data[data['GRAPHIC_TYPE'] == 'B']) == 1:
                resolution_pfi = data[data['GRAPHIC_TYPE'] == 'B'].index.values[0]
                resolution_code = 'BASE_PROP'
                resolution_desc = 'SINGLE BASE_PROP'

            # rule #3: single APPROVED PROPERTY_STATUS
            elif len(data[data['STATUS'] == 'A']) == 1:
                resolution_pfi = data[data['STATUS'] == 'A'].index.values[0]
                resolution_code = 'APPROVED'
                resolution_desc = 'SINGLE APPROVED'

            # rule 4: single COMMON PROPERTY
            elif len(data[data['LV_APT'].isnull()]) == 1:
                resolution_pfi = data[data['LV_APT'].isnull()].index.values[0]
                resolution_code = 'BASE_ADD'
                resolution_desc = 'SINGLE COMMON PROPERTY'

            # rule 5: same location
            elif len(pd.unique([s.ToString() for s in data['SHAPE']])) == 1:                            
                resolution_pfi = data.index.values[0]
                resolution_code = 'SAME_POINT'
                resolution_desc = 'ADDRESS LOCATION ALL THE SAME'

            # rule 6: same ROAD_PFI
            elif len(pd.unique(data['ROAD_PFI'])) == 1:
                
                sub_data = data[data['IS_PRIMARY'] == 'Y']
                
                if len(sub_data) > 0:
                    # sub-rule 1: has IS_PRIMARY
                    resolution_pfi = sub_data.index.values[0]
                    resolution_code = 'RD_SING_PR'
                    resolution_desc = 'SAME ROAD SELECT IS_PRIMARY'
                    
                else:
                    resolution_pfi = data.index.values[0]
                    resolution_code = 'RD_SINGANY'
                    resolution_desc = 'SAME ROAD ANY'
            
            else:
                resolution_pfi = None
                resolution_code = 'UNRESOLVED'
                resolution_desc = 'PHASE 1 UNRESOLVED'

            # load data into ADDRESS_DUPLICATE_RESOLUTION
            for pfi in data.index.values:
                sbc_res.add_row((pfi, resolution_pfi, resolution_code, resolution_desc, data['ADDRESS_STRING'].values[0]))
                                    
            if enum % 10000 == 0:
                logging.info((enum, len(temp)))
                sbc_res.flush()
        logging.info((enum, len(temp)))

    logging.info('creating summary: ADDRESS_DUPLICATE_RESOLUTION_PHASE_1_SUMMARY')
    with conn.begin():
        conn.execute('''
        SELECT
            RESOLUTION_CODE,
            COUNT(*) AS COUNT
        INTO ADDRESS_DUPLICATE_RESOLUTION_PHASE_1_SUMMARY
        FROM ADDRESS_DUPLICATE_RESOLUTION_PHASE_1
        GROUP BY RESOLUTION_CODE
        ''')

    logging.info('creating summary: ADDRESS_DUPLICATE_RESOLUTION_PHASE_1_SUMMARY_UNIQUE')
    with conn.begin():
        conn.execute('''
        SELECT
            RESOLUTION_CODE,
            COUNT(DISTINCT RESOLUTION_PFI) AS COUNT
        INTO ADDRESS_DUPLICATE_RESOLUTION_PHASE_1_SUMMARY_UNIQUE
        FROM ADDRESS_DUPLICATE_RESOLUTION_PHASE_1
        GROUP BY RESOLUTION_CODE
        ''')

    logging.info('create ADDRESS_VALIDATED_PHASE_1')
    with conn.begin():
        
        conn.execute('''
        SELECT DISTINCT PFI
        INTO ADDRESS_VALIDATED_PHASE_1
        FROM ADDRESS
        EXCEPT
        (
            SELECT DISTINCT PFI FROM ADDRESS_EXCLUSION
            UNION
            SELECT DISTINCT PFI FROM ADDRESS_DUPLICATE_RESOLUTION_PHASE_1                       
            EXCEPT
            SELECT DISTINCT RESOLUTION_PFI AS PFI 
            FROM ADDRESS_DUPLICATE_RESOLUTION_PHASE_1
            WHERE RESOLUTION_CODE <> 'UNRESOLVED'
        )
        ''')


def calc_road_ranges_phase_1(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_sqlalchemy(em.server, em.database_name, init_geomtype='clr')

    logging.info('dropping tables:')
    if dbpy.check_exists('ROAD_RANGE_LEFT_PHASE_1', conn):
        logging.info('ROAD_RANGE_LEFT_PHASE_1')
        conn.execute('drop table ROAD_RANGE_LEFT_PHASE_1')
    if dbpy.check_exists('ROAD_RANGE_RIGHT_PHASE_1', conn):
        logging.info('ROAD_RANGE_RIGHT_PHASE_1')
        conn.execute('drop table ROAD_RANGE_RIGHT_PHASE_1')
    if dbpy.check_exists('ROAD_RANGING_PHASE_1', conn):
        logging.info('ROAD_RANGING_PHASE_1')
        conn.execute('drop table ROAD_RANGING_PHASE_1')
        

    logging.info('creating ROAD_RANGE_LEFT_PHASE_1')
    with conn.begin():
        conn.execute('''
        SELECT 
            R.PFI,
            R.ROAD_NAME,
            R.ROAD_TYPE,
            R.ROAD_SUFFIX,
            R.LEFT_LOCALITY,
            R.RIGHT_LOCALITY,
            MIN(A.HOUSE_NUMBER_1) AS ADDRESS_LEFT_MIN,
            MAX(ISNULL(A.HOUSE_NUMBER_2, A.HOUSE_NUMBER_1)) AS ADDRESS_LEFT_MAX,
            --max(A.HOUSE_NUMBER_2) as test,
            MIN(A.HOUSE_NUMBER_1 % 2) AS ADDRESS_LEFT_ODD_MIN,
            MAX(A.HOUSE_NUMBER_1 % 2) AS ADDRESS_LEFT_ODD_MAX
        INTO ROAD_RANGE_LEFT_PHASE_1
        FROM ROAD R
        LEFT JOIN ADDRESS_ROAD_VALIDATION AR 
        ON R.PFI = AR.ROAD_PFI and
           AR.SIDE_OF_ROAD = 'L'
        INNER JOIN ADDRESS_VALIDATED_PHASE_1 A1 ON
            AR.ADDR_PFI = A1.PFI
        INNER JOIN ADDRESS A
        ON AR.ADDR_PFI = A.PFI
        WHERE
            AR.RULE_SCORE >= 50
        GROUP BY
            R.PFI,
            R.ROAD_NAME,
            R.ROAD_TYPE,
            R.ROAD_SUFFIX,
            R.LEFT_LOCALITY,
            R.RIGHT_LOCALITY
        ''')
    
    logging.info('creating ROAD_RANGE_RIGHT_PHASE_1')
    with conn.begin():
        conn.execute('''
        SELECT 
            R.PFI,
            R.ROAD_NAME,
            R.ROAD_TYPE,
            R.ROAD_SUFFIX,
            R.LEFT_LOCALITY,
            R.RIGHT_LOCALITY,
            MIN(A.HOUSE_NUMBER_1) AS ADDRESS_RIGHT_MIN,
            MAX(ISNULL(A.HOUSE_NUMBER_2, A.HOUSE_NUMBER_1)) AS ADDRESS_RIGHT_MAX,
            MIN(A.HOUSE_NUMBER_1 % 2) AS ADDRESS_RIGHT_ODD_MIN,
            MAX(A.HOUSE_NUMBER_1 % 2) AS ADDRESS_RIGHT_ODD_MAX
        INTO ROAD_RANGE_RIGHT_PHASE_1
        FROM ROAD R
        LEFT JOIN ADDRESS_ROAD_VALIDATION AR 
        ON R.PFI = AR.ROAD_PFI and
           AR.SIDE_OF_ROAD = 'R'
        INNER JOIN ADDRESS_VALIDATED_PHASE_1 A1 ON
            AR.ADDR_PFI = A1.PFI
        INNER JOIN ADDRESS A
        ON AR.ADDR_PFI = A.PFI
        WHERE
            AR.RULE_SCORE >= 50
        GROUP BY
            R.PFI,
            R.ROAD_NAME,
            R.ROAD_TYPE,
            R.ROAD_SUFFIX,
            R.LEFT_LOCALITY,
            R.RIGHT_LOCALITY
        ''')

    logging.info('creating ROAD_RANGING_PHASE_1')
    conn.execute('''
    CREATE TABLE [dbo].[ROAD_RANGING_PHASE_1](
        [PFI] [int] NOT NULL,
        [ADDRESS_LEFT_MIN] [int] NULL,
        [ADDRESS_LEFT_MAX] [int] NULL,
        [ADDRESS_RIGHT_MIN] [int] NULL,
        [ADDRESS_RIGHT_MAX] [int] NULL,
        [ADDRESS_LEFT_ODD_MIN] [int] NULL,
        [ADDRESS_LEFT_ODD_MAX] [int] NULL,
        [ADDRESS_RIGHT_ODD_MIN] [int] NULL,
        [ADDRESS_RIGHT_ODD_MAX] [int] NULL,
        [ADDRESS_TYPE] [int] NULL
    ) ON [PRIMARY]
    ''')
    
    logging.info('insert initial')
    with conn.begin():
        conn.execute('INSERT INTO ROAD_RANGING_PHASE_1 (PFI) SELECT PFI FROM ROAD')

    logging.info('updating LEFT')
    with conn.begin():
        conn.execute('''
        UPDATE RD SET
            RD.ADDRESS_LEFT_MIN = RRL.ADDRESS_LEFT_MIN,
            RD.ADDRESS_LEFT_MAX = RRL.ADDRESS_LEFT_MAX,
            RD.ADDRESS_LEFT_ODD_MIN = RRL.ADDRESS_LEFT_ODD_MIN,
            RD.ADDRESS_LEFT_ODD_MAX = RRL.ADDRESS_LEFT_ODD_MAX
        FROM 
            ROAD_RANGING_PHASE_1 RD
            INNER JOIN ROAD_RANGE_LEFT_PHASE_1 RRL
            ON RD.PFI = RRL.PFI
        ''')

    logging.info('updating RIGHT')
    with conn.begin():
        conn.execute('''
        UPDATE RD SET
            RD.ADDRESS_RIGHT_MIN = RRR.ADDRESS_RIGHT_MIN,
            RD.ADDRESS_RIGHT_MAX = RRR.ADDRESS_RIGHT_MAX,
            RD.ADDRESS_RIGHT_ODD_MIN = RRR.ADDRESS_RIGHT_ODD_MIN,
            RD.ADDRESS_RIGHT_ODD_MAX = RRR.ADDRESS_RIGHT_ODD_MAX
        FROM 
            ROAD_RANGING_PHASE_1 RD
            INNER JOIN ROAD_RANGE_RIGHT_PHASE_1 RRR
            ON RD.PFI = RRR.PFI
        ''')

    logging.info('patching')
    with conn.begin():
        conn.execute('''
        -- MANUAL FIX FOR SPRINGVALE ROAD NUNAWADING ESTA CR 1024
        -- LOGGED ON DSE NES #7070. Status = pending @ 20090721
        UPDATE RD SET
            RD.ADDRESS_LEFT_MIN = 1,
            RD.ADDRESS_LEFT_MAX = 3,
            RD.ADDRESS_RIGHT_MIN = 14,
            RD.ADDRESS_RIGHT_MAX = 18,
            RD.ADDRESS_LEFT_ODD_MIN = 1,
            RD.ADDRESS_LEFT_ODD_MAX = 1,
            RD.ADDRESS_RIGHT_ODD_MIN = 0,
            RD.ADDRESS_RIGHT_ODD_MAX = 0
        FROM 
            ROAD_RANGING_PHASE_1 RD
        WHERE PFI = 5671261
        ''')

    logging.info('updating ADDRESS_TYPE')
    with conn.begin():
        conn.execute('''
        UPDATE ROAD_RANGING_PHASE_1 SET
            ADDRESS_TYPE = 
            CASE
            
            --LEFT SIDE IS THE SAME PARITY AND --RIGHT SIDE HAS NO VALUE
            WHEN 
                (ADDRESS_LEFT_ODD_MIN = ADDRESS_LEFT_ODD_MAX) AND 
                (ADDRESS_RIGHT_ODD_MIN IS NULL AND ADDRESS_RIGHT_ODD_MAX IS NULL) 
            THEN 0
            
            --RIGHT SIDE IS THE SAME PARITY AND --LEFT SIDE HAS NO VALUE
            WHEN
                (ADDRESS_RIGHT_ODD_MIN = ADDRESS_RIGHT_ODD_MAX) AND 
                (ADDRESS_LEFT_ODD_MIN IS NULL AND ADDRESS_LEFT_ODD_MAX IS NULL) 
            THEN 0

            -- LEFT AND RIGHT SIDE IS THE SAME PARITY AND DIFFERENT PARITY
            WHEN 
                (ADDRESS_LEFT_ODD_MIN = ADDRESS_LEFT_ODD_MAX) AND --LEFT SIDE IS THE SAME PARITY
                (ADDRESS_RIGHT_ODD_MIN = ADDRESS_RIGHT_ODD_MAX) AND --RIGHT SIDE IS THE SAME PARITY
                (ADDRESS_LEFT_ODD_MIN <> ADDRESS_RIGHT_ODD_MIN) -- LEFT AND RIGHT SIDE HAS DIFFERENT PARITY
            THEN 0

            -- NO ADDRESS DATA
            WHEN
                ADDRESS_LEFT_ODD_MIN IS NULL OR 
                ADDRESS_LEFT_ODD_MAX IS NULL OR
                ADDRESS_RIGHT_ODD_MIN IS NULL OR 
                ADDRESS_RIGHT_ODD_MAX IS NULL
            THEN 1

            -- ALL OTHER TYPES
            ELSE 1
            END
        ''')


def address_validation_phase_2(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_sqlalchemy(em.server, em.database_name, init_geomtype='clr')

    logging.info('dropping tables:')
    if dbpy.check_exists('ROAD_INFRA_LINK', conn):
        logging.info('ROAD_INFRA_LINK')
        conn.execute('drop table ROAD_INFRA_LINK')
    if dbpy.check_exists('ROAD_RANGE_NEAR', conn):
        logging.info('ROAD_RANGE_NEAR')
        conn.execute('drop table ROAD_RANGE_NEAR')
    if dbpy.check_exists('ROAD_RANGE_NEAR_GROUP', conn):
        logging.info('ROAD_RANGE_NEAR_GROUP')
        conn.execute('drop table ROAD_RANGE_NEAR_GROUP')
    if dbpy.check_exists('ADDRESS_DUPLICATE_RESOLUTION_PHASE_2', conn):
        logging.info('ADDRESS_DUPLICATE_RESOLUTION_PHASE_2')
        conn.execute('drop table ADDRESS_DUPLICATE_RESOLUTION_PHASE_2')
    if dbpy.check_exists('ADDRESS_DUPLICATE_RESOLUTION_PHASE_2_SUMMARY', conn):
        logging.info('ADDRESS_DUPLICATE_RESOLUTION_PHASE_2_SUMMARY')
        conn.execute('drop table ADDRESS_DUPLICATE_RESOLUTION_PHASE_2_SUMMARY')
    if dbpy.check_exists('ADDRESS_DUPLICATE_RESOLUTION_PHASE_2_SUMMARY_UNIQUE', conn):
        logging.info('ADDRESS_DUPLICATE_RESOLUTION_PHASE_2_SUMMARY_UNIQUE')
        conn.execute('drop table ADDRESS_DUPLICATE_RESOLUTION_PHASE_2_SUMMARY_UNIQUE')
    if dbpy.check_exists('ADDRESS_VALIDATED_PHASE_2', conn):
        logging.info('ADDRESS_VALIDATED_PHASE_2')
        conn.execute('drop table ADDRESS_VALIDATED_PHASE_2')
        

        
    logging.info('creating ROAD_INFRA_LINK')
    with conn.begin():
        conn.execute('''
        SELECT
            PFI AS ROAD_PFI,
            UFI AS ROAD_INFRA_UFI
        INTO ROAD_INFRA_LINK
        FROM
            (SELECT PFI, FROM_UFI AS UFI FROM ROAD
             UNION ALL
             SELECT PFI, TO_UFI AS UFI FROM ROAD) T
        ''')

    logging.info('creating ROAD_RANGE_NEAR')
    with conn.begin():
        conn.execute('''
        SELECT DISTINCT
            RA.PFI,
            RA.ROAD_NAME_ID,

            -- MIN NUMBER
            CASE
            WHEN ISNULL(RR.ADDRESS_LEFT_MIN, 99999999) < ISNULL(RR.ADDRESS_RIGHT_MIN, 99999999)
            THEN RR.ADDRESS_LEFT_MIN
            ELSE RR.ADDRESS_RIGHT_MIN
            END AS NUMBER_MIN,

            -- MAX NUMBER
            CASE
            WHEN ISNULL(RR.ADDRESS_LEFT_MAX, 0) > ISNULL(RR.ADDRESS_RIGHT_MAX, 0)
            THEN RR.ADDRESS_LEFT_MAX
            ELSE RR.ADDRESS_RIGHT_MAX
            END AS NUMBER_MAX
            
        INTO ROAD_RANGE_NEAR

        FROM ROAD_ALIAS RA
        INNER JOIN ROAD_INFRA_LINK RI
            ON RA.PFI = RI.ROAD_PFI
        INNER JOIN ROAD_INFRA_LINK RI2
            ON RI.ROAD_INFRA_UFI = RI2.ROAD_INFRA_UFI
        INNER JOIN ROAD R
            ON RI2.ROAD_PFI = R.PFI
        INNER JOIN ROAD_ALIAS RA2
            ON RI2.ROAD_PFI = RA2.PFI AND 
            RA.ROAD_NAME_ID = RA2.ROAD_NAME_ID
        INNER JOIN ROAD_RANGING_PHASE_1 RR
            ON R.PFI = RR.PFI
        ''')

    logging.info('creating ROAD_RANGE_NEAR_GROUP')
    with conn.begin():
        conn.execute('''
        SELECT
            PFI,
            ROAD_NAME_ID,
            MIN(NUMBER_MIN) AS NUMBER_MIN,
            MAX(NUMBER_MAX) AS NUMBER_MAX

        INTO ROAD_RANGE_NEAR_GROUP
        
        FROM ROAD_RANGE_NEAR
        GROUP BY
            PFI, ROAD_NAME_ID
        ''')

    logging.info('creating ADDRESS_DUPLICATE_RESOLUTION_PHASE_2')
    with conn.begin():
        conn.execute('''
        CREATE TABLE [dbo].[ADDRESS_DUPLICATE_RESOLUTION_PHASE_2](
            [PFI] [nvarchar](10) NOT NULL,
            [RESOLUTION_PFI] [nvarchar](10) NULL,
            [RESOLUTION_CODE] [nvarchar](20) NULL,
            [RESOLUTION_DESC] [nvarchar](255) NULL,
            [ADDRESS_STRING] [nvarchar](255) NULL,
            [ROAD_PFI] [nvarchar](255) NULL
        ) ON [PRIMARY]
        ''')


    logging.info('reading ROAD_RANGE_NEAR_GROUP')
    rrng_data = pd.read_sql('''
    SELECT
        PFI,
        ROAD_NAME_ID,
        NUMBER_MIN,
        NUMBER_MAX
    FROM
    ROAD_RANGE_NEAR_GROUP
    ''', conn)
    logging.info('setting index on PFI')
    rrng_data.set_index('PFI', drop=False, inplace=True)
    
    logging.info('reading ADDRESS_VALIDATION')
    address_duplicates_data = pd.read_sql('''
    SELECT
        AV.*,
        --RR.*
        RR.ADDRESS_LEFT_MIN,
        RR.ADDRESS_LEFT_MAX,
        RR.ADDRESS_RIGHT_MIN,
        RR.ADDRESS_RIGHT_MAX,
        RR.ADDRESS_LEFT_ODD_MIN,
        RR.ADDRESS_LEFT_ODD_MAX,
        RR.ADDRESS_RIGHT_ODD_MIN,
        RR.ADDRESS_RIGHT_ODD_MAX,
        RR.ADDRESS_TYPE
    FROM ADDRESS_VALIDATION AV
    LEFT JOIN ADDRESS_DUPLICATE_RESOLUTION_PHASE_1 D
    ON AV.PFI = D.PFI
    LEFT JOIN ROAD_RANGING_PHASE_1 RR
    ON AV.ROAD_PFI = RR.PFI
    WHERE D.RESOLUTION_CODE = 'UNRESOLVED'
    ''', conn)
    logging.info('setting index on PFI')
    address_duplicates_data.set_index('PFI', drop=False, inplace=True)

    logging.info('UNRESOLVED: {}'.format(len(address_duplicates_data)))

    grouped = address_duplicates_data.groupby(by=[
        'ST_NUM',
        'ROAD_NAME',
        'ROAD_TYPE',
        'ROAD_SUFFIX',
        'LOCALITY_NAME'])

    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'ADDRESS_DUPLICATE_RESOLUTION_PHASE_2') as sbc:

        logging.info('groups: {}'.format(len(grouped.groups)))
        for enum, (item, pfis) in enumerate(grouped.groups.iteritems()):

            # get address data
            data = address_duplicates_data.ix[pfis].sort_values(by=['IS_PRIMARY', 'RULE_SCORE', 'DIST_FROM_ROAD', 'HOUSE_NUMBER_2', 'LV_APT', 'PFI'],
                                                                ascending=[False, False, True, False, True, False])
            # get road pfi
            road_data = rrng_data.ix[data['ROAD_PFI']]

            # conditions
            c_address_parity = data['ADDRESS_TYPE'] == 0
            c_left_min_bound = data['ADDRESS_LEFT_MIN'] <= data['HOUSE_NUMBER_1']
            c_left_max_bound = data['ADDRESS_LEFT_MAX'] >= data['HOUSE_NUMBER_1']
            c_left_parity = data['ADDRESS_LEFT_MIN'] % 2 == data['HOUSE_NUMBER_1'] % 2
            c_right_min_bound = data['ADDRESS_RIGHT_MIN'] <= data['HOUSE_NUMBER_1']
            c_right_max_bound = data['ADDRESS_RIGHT_MAX'] >= data['HOUSE_NUMBER_1']
            c_right_parity = data['ADDRESS_RIGHT_MIN'] % 2 == data['HOUSE_NUMBER_1'] % 2
            c_same_rnid = data['ROAD_NAME_ID'] == data['ROAD_NAME_ID'].unique()[0]
            c_left_min_isnull = data['ADDRESS_LEFT_MIN'].isnull()
            c_left_max_isnull = data['ADDRESS_LEFT_MAX'].isnull()
            c_diff_right_parity = data['ADDRESS_RIGHT_MIN'] % 2 <> data['HOUSE_NUMBER_1'] % 2
            c_right_min_isnull = data['ADDRESS_RIGHT_MIN'].isnull()
            c_right_max_isnull = data['ADDRESS_RIGHT_MAX'].isnull()
            c_diff_left_parity = data['ADDRESS_LEFT_MIN'] % 2 <> data['HOUSE_NUMBER_1'] % 2

            c_road_min_bound = road_data['NUMBER_MIN'] <= data['HOUSE_NUMBER_1'].unique()[0]
            c_road_max_bound = road_data['NUMBER_MAX'] >= data['HOUSE_NUMBER_1'].unique()[0]
            c_road_same_rnid = road_data['ROAD_NAME_ID'] == data['ROAD_NAME_ID'].unique()[0]
            c_road_min_bound_within10 = road_data['NUMBER_MIN'] <= data['HOUSE_NUMBER_1'].unique()[0] + 10
            c_road_max_bound_within10 = road_data['NUMBER_MAX'] >= data['HOUSE_NUMBER_1'].unique()[0] - 10
            
            if len(data.ix[c_address_parity &
                           ((c_left_min_bound &
                             c_left_max_bound &
                             c_left_parity) |
                            (c_right_min_bound &
                             c_right_max_bound &
                             c_right_parity))]) > 0:
                resolution_code = 'R*_NULL'
                road_pfi = data.ix[c_address_parity &
                                   ((c_left_min_bound &
                                     c_left_max_bound &
                                     c_left_parity) |
                                    (c_right_min_bound &
                                     c_right_max_bound &
                                     c_right_parity))]['ROAD_PFI'].values[0]
                
            elif len(data.ix[(c_left_min_bound & c_left_max_bound) |
                             (c_right_min_bound & c_right_max_bound)]) > 0:
                
                resolution_code = 'R*_RNG'
                road_pfi = data.ix[(c_left_min_bound & c_left_max_bound) |
                                   (c_right_min_bound & c_right_max_bound)]['ROAD_PFI'].values[0]

            elif len(road_data.ix[c_road_same_rnid &
                                  (c_road_min_bound & c_road_max_bound)]) > 0:
                resolution_code = 'R*_RNGNEAR'
                road_pfi = road_data.ix[c_road_same_rnid &
                                        (c_road_min_bound & c_road_max_bound)]['PFI'].values[0]

            elif len(data.ix[c_address_parity &
                             (c_left_min_isnull & c_left_max_isnull & c_diff_right_parity) |
                             (c_right_min_isnull & c_right_max_isnull & c_diff_left_parity)]) > 0:
                resolution_code = 'R*_1S_TY'
                road_pfi = data.ix[c_address_parity &
                                   (c_left_min_isnull & c_left_max_isnull & c_diff_right_parity) |
                                   (c_right_min_isnull & c_right_max_isnull & c_diff_left_parity)
                                   ]['ROAD_PFI'].values[0]

            elif len(data.ix[(c_left_min_isnull & c_left_max_isnull & c_diff_right_parity) |
                             (c_right_min_isnull & c_right_max_isnull & c_diff_left_parity)]) > 0:
                resolution_code = 'R*_1S'
                road_pfi = data.ix[(c_left_min_isnull & c_left_max_isnull & c_diff_right_parity) |
                                   (c_right_min_isnull & c_right_max_isnull & c_diff_left_parity)
                                   ]['ROAD_PFI'].values[0]
            elif len(data.ix[c_left_min_isnull & c_left_max_isnull &
                             c_right_min_isnull & c_right_max_isnull]) > 0:
                resolution_code = 'R*_ALL_NUL'
                road_pfi = data.ix[c_left_min_isnull & c_left_max_isnull &
                                   c_right_min_isnull & c_right_max_isnull]['ROAD_PFI'].values[0]

            elif len(road_data.ix[c_road_same_rnid & (c_road_min_bound_within10 & c_road_max_bound_within10)]) > 0:
                resolution_code = 'R*_RNG_OFF'
                road_pfi = road_data.ix[c_road_same_rnid & (c_road_min_bound_within10 & c_road_max_bound_within10)]['PFI'].values[0]

            else: 
                resolution_code = None
                road_pfi = None

            resolution_data = data.ix[data['ROAD_PFI'] == road_pfi]
            resolution_pfi = None
            if len(resolution_data) > 0:
                resolution_pfi = resolution_data['PFI'].values[0]

            if resolution_code is None:
                resolution_code = 'RANDOM'
                resolution_pfi = data['PFI'].values[0]
                road_pfi = data['ROAD_PFI'].values[0]
            resolution_desc = ''
                                    
            # load data into ADDRESS_DUPLICATE_RESOLUTION
            for pfi in data.index.values:
                
                # road_pfi is numpy.int64, thus using int() on it.
                sbc.add_row((pfi, resolution_pfi, resolution_code, resolution_desc, data['ADDRESS_STRING'].values[0], int(road_pfi)))

            if enum % 100 == 0:
                logging.info(enum)
                sbc.flush()
        logging.info(enum)

    logging.info('creating summary: ADDRESS_DUPLICATE_RESOLUTION_PHASE_2_SUMMARY')
    with conn.begin():
        conn.execute('''
        SELECT
            RESOLUTION_CODE,
            COUNT(*) AS COUNT
        INTO ADDRESS_DUPLICATE_RESOLUTION_PHASE_2_SUMMARY
        FROM ADDRESS_DUPLICATE_RESOLUTION_PHASE_2
        GROUP BY RESOLUTION_CODE
        ''')

    logging.info('creating summary: ADDRESS_DUPLICATE_RESOLUTION_PHASE_2_SUMMARY_UNIQUE')
    with conn.begin():
        conn.execute('''
        SELECT
            RESOLUTION_CODE,
            COUNT(DISTINCT RESOLUTION_PFI) AS COUNT
        INTO ADDRESS_DUPLICATE_RESOLUTION_PHASE_2_SUMMARY_UNIQUE
        FROM ADDRESS_DUPLICATE_RESOLUTION_PHASE_2
        GROUP BY RESOLUTION_CODE
        ''')

    logging.info('create ADDRESS_VALIDATED_PHASE_2')
    with conn.begin():
        conn.execute('''
        SELECT DISTINCT PFI
        INTO ADDRESS_VALIDATED_PHASE_2
        FROM ADDRESS
        EXCEPT
        (
            SELECT DISTINCT PFI FROM ADDRESS_EXCLUSION
            UNION
            SELECT DISTINCT PFI FROM ADDRESS_DUPLICATE_RESOLUTION_PHASE_1
            EXCEPT
            SELECT DISTINCT RESOLUTION_PFI AS PFI 
            FROM ADDRESS_DUPLICATE_RESOLUTION_PHASE_1
            WHERE RESOLUTION_CODE <> 'UNRESOLVED'
            EXCEPT
            SELECT DISTINCT RESOLUTION_PFI AS PFI
            FROM ADDRESS_DUPLICATE_RESOLUTION_PHASE_2
         )
         ''')


def export_address_validated(estamap_version):
    # from sql script: 300_AddressDuplicationPatches.sql
    # hard coded here, look at alternative to hardcoded patching.
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_sqlalchemy(em.server, em.database_name, init_geomtype='clr')

    logging.info('dropping tables:')
    if dbpy.check_exists('ADDRESS_VALIDATED_FINAL', conn):
        logging.info('ADDRESS_VALIDATED_FINAL')
        conn.execute('drop table ADDRESS_VALIDATED_FINAL')
    if dbpy.check_exists('ADDRESS_DUPLICATE_OVERRIDE', conn):
        logging.info('ADDRESS_DUPLICATE_OVERRIDE')
        conn.execute('drop table ADDRESS_DUPLICATE_OVERRIDE')


    logging.info('creating ADDRESS_DUPLICATE_OVERRIDE')
    with conn.begin():
        conn.execute('''
        CREATE TABLE [dbo].[ADDRESS_DUPLICATE_OVERRIDE](
            [ADDRESS_STRING] [nvarchar](255) NULL,
            [OVERRIDE_PFI] [nvarchar] (20) NULL,
            [REFERENCE] [nvarchar](255) NULL
        ) ON [PRIMARY]
        ''')

    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'ADDRESS_DUPLICATE_OVERRIDE') as sbc:
        sbc.add_row(('BOX HILL_WHITEHORSE_RD__1022', 209400237, 'INC 36280'))
        sbc.add_row(('SOUTH YARRA_CHAPEL_ST__531', 214010233, 'INC 40689'))
        sbc.add_row(('BANDIANA_ANZAC_PDE__4227', 53613752, 'SPPT'))
        sbc.add_row(('FRANKSTON NORTH_MORNINGTON PENINSULA_FWY__1', 54663219, 'ITSM 51761 and ITSM 52972'))
        sbc.add_row(('BRIGHTON_KINANE_ST__18', 53008005, 'ITSM 59220'))
        sbc.add_row(('NUMURKAH_KATAMATITE-NATHALIA_RD__2', 126476619, 'HOSPITAL'))
        sbc.add_row(('LANGWARRIN_MCCLELLAND_DR__80', 51764311, 'REF0024886'))

    logging.info('create ADDRESS_VALIDATED_FINAL')
    with conn.begin():
        
        conn.execute('''
        SELECT DISTINCT PFI
        INTO ADDRESS_VALIDATED_FINAL
        FROM ADDRESS
        WHERE
            PFI IN
            (
            SELECT DISTINCT PFI FROM ADDRESS_VALIDATED_PHASE_2
            UNION
            SELECT OVERRIDE_PFI FROM ADDRESS_DUPLICATE_OVERRIDE
            )
        ''')


def register_new_address(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)

    count_before = conn.execute('SELECT COUNT(*) FROM ADDRESS_MSLINK_REGISTER').fetchval()
    
    logging.info('register new ADDRESS')
    conn.execute('''
    INSERT INTO DBO.ADDRESS_MSLINK_REGISTER (CAD_STRING, SOURCE_DATASET, SOURCE_PK)
    SELECT
        ISNULL(A.LOCALITY_NAME,'')  + '_' + ISNULL(RN.ROAD_NAME,'') + '_' + ISNULL(RN.ROAD_TYPE,'') + '_' + ISNULL(RN.ROAD_SUFFIX,'') + '_' +  ISNULL(A.HOUSE_PREFIX_1,'') + CAST(A.HOUSE_NUMBER_1 AS VARCHAR(11)) + ISNULL(HOUSE_SUFFIX_1,'') + '_' as ADDRESS_STRING,
        'VICMAP_{vm}.ADDRESS',
        A.PFI
    FROM ADDRESS A
    INNER JOIN ADDRESS_VALIDATED_FINAL V
    ON A.PFI = V.PFI
    LEFT JOIN ADDRESS_RNID AI
    ON A.PFI = AI.PFI
    LEFT JOIN ROAD_NAME_REGISTER RN
    ON AI.ROAD_NAME_ID = RN.ROAD_NAME_ID
    WHERE
        ISNULL(A.LOCALITY_NAME,'')  + '_' + ISNULL(RN.ROAD_NAME,'') + '_' + ISNULL(RN.ROAD_TYPE,'') + '_' + ISNULL(RN.ROAD_SUFFIX,'') + '_' +  ISNULL(A.HOUSE_PREFIX_1,'') + CAST(A.HOUSE_NUMBER_1 AS VARCHAR(11)) + ISNULL(HOUSE_SUFFIX_1,'') + '_' 
        NOT IN (SELECT CAD_STRING FROM ADDRESS_MSLINK_REGISTER)
    '''.format(vm=em.vicmap_version))
    
    count_after = conn.execute('SELECT COUNT(*) FROM ADDRESS_MSLINK_REGISTER').fetchval()

    logging.info('count before: {}'.format(count_before))
    logging.info('count after: {}'.format(count_after))
    logging.info('new addresses: {}'.format(count_after - count_before))
    conn.commit()


def calc_road_ranges(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_sqlalchemy(em.server, em.database_name, init_geomtype='clr')

    logging.info('dropping tables:')
    if dbpy.check_exists('ROAD_RANGE_LEFT', conn):
        logging.info('ROAD_RANGE_LEFT')
        conn.execute('drop table ROAD_RANGE_LEFT')
    if dbpy.check_exists('ROAD_RANGE_RIGHT', conn):
        logging.info('ROAD_RANGE_RIGHT')
        conn.execute('drop table ROAD_RANGE_RIGHT')
    if dbpy.check_exists('ROAD_RANGING', conn):
        logging.info('ROAD_RANGING')
        conn.execute('drop table ROAD_RANGING')
        

    logging.info('creating ROAD_RANGE_LEFT')
    with conn.begin():
        conn.execute('''
        SELECT 
            R.PFI,
            R.ROAD_NAME,
            R.ROAD_TYPE,
            R.ROAD_SUFFIX,
            R.LEFT_LOCALITY,
            R.RIGHT_LOCALITY,
            MIN(A.HOUSE_NUMBER_1) AS ADDRESS_LEFT_MIN,
            MAX(ISNULL(A.HOUSE_NUMBER_2, A.HOUSE_NUMBER_1)) AS ADDRESS_LEFT_MAX,
            --max(A.HOUSE_NUMBER_2) as test,
            MIN(A.HOUSE_NUMBER_1 % 2) AS ADDRESS_LEFT_ODD_MIN,
            MAX(A.HOUSE_NUMBER_1 % 2) AS ADDRESS_LEFT_ODD_MAX
        INTO ROAD_RANGE_LEFT
        FROM ROAD R
        LEFT JOIN ADDRESS_ROAD_VALIDATION AR 
        ON R.PFI = AR.ROAD_PFI and
           AR.SIDE_OF_ROAD = 'L'
        INNER JOIN ADDRESS_VALIDATED_FINAL A1 ON
            AR.ADDR_PFI = A1.PFI
        INNER JOIN ADDRESS A
        ON AR.ADDR_PFI = A.PFI
        WHERE
            AR.RULE_SCORE >= 50
        GROUP BY
            R.PFI,
            R.ROAD_NAME,
            R.ROAD_TYPE,
            R.ROAD_SUFFIX,
            R.LEFT_LOCALITY,
            R.RIGHT_LOCALITY
        ''')
    
    logging.info('creating ROAD_RANGE_RIGHT')
    with conn.begin():
        conn.execute('''
        SELECT 
            R.PFI,
            R.ROAD_NAME,
            R.ROAD_TYPE,
            R.ROAD_SUFFIX,
            R.LEFT_LOCALITY,
            R.RIGHT_LOCALITY,
            MIN(A.HOUSE_NUMBER_1) AS ADDRESS_RIGHT_MIN,
            MAX(ISNULL(A.HOUSE_NUMBER_2, A.HOUSE_NUMBER_1)) AS ADDRESS_RIGHT_MAX,
            MIN(A.HOUSE_NUMBER_1 % 2) AS ADDRESS_RIGHT_ODD_MIN,
            MAX(A.HOUSE_NUMBER_1 % 2) AS ADDRESS_RIGHT_ODD_MAX
        INTO ROAD_RANGE_RIGHT
        FROM ROAD R
        LEFT JOIN ADDRESS_ROAD_VALIDATION AR 
        ON R.PFI = AR.ROAD_PFI and
           AR.SIDE_OF_ROAD = 'R'
        INNER JOIN ADDRESS_VALIDATED_FINAL A1 ON
            AR.ADDR_PFI = A1.PFI
        INNER JOIN ADDRESS A
        ON AR.ADDR_PFI = A.PFI
        WHERE
            AR.RULE_SCORE >= 50
        GROUP BY
            R.PFI,
            R.ROAD_NAME,
            R.ROAD_TYPE,
            R.ROAD_SUFFIX,
            R.LEFT_LOCALITY,
            R.RIGHT_LOCALITY
        ''')

    logging.info('creating ROAD_RANGING')
    conn.execute('''
    CREATE TABLE [dbo].[ROAD_RANGING](
        [PFI] [int] NOT NULL,
        [ADDRESS_LEFT_MIN] [int] NULL,
        [ADDRESS_LEFT_MAX] [int] NULL,
        [ADDRESS_RIGHT_MIN] [int] NULL,
        [ADDRESS_RIGHT_MAX] [int] NULL,
        [ADDRESS_LEFT_ODD_MIN] [int] NULL,
        [ADDRESS_LEFT_ODD_MAX] [int] NULL,
        [ADDRESS_RIGHT_ODD_MIN] [int] NULL,
        [ADDRESS_RIGHT_ODD_MAX] [int] NULL,
        [ADDRESS_TYPE] [int] NULL
    ) ON [PRIMARY]
    ''')
    
    logging.info('insert initial')
    with conn.begin():
        conn.execute('INSERT INTO ROAD_RANGING (PFI) SELECT PFI FROM ROAD')

    logging.info('updating LEFT')
    with conn.begin():
        conn.execute('''
        UPDATE RD SET
            RD.ADDRESS_LEFT_MIN = RRL.ADDRESS_LEFT_MIN,
            RD.ADDRESS_LEFT_MAX = RRL.ADDRESS_LEFT_MAX,
            RD.ADDRESS_LEFT_ODD_MIN = RRL.ADDRESS_LEFT_ODD_MIN,
            RD.ADDRESS_LEFT_ODD_MAX = RRL.ADDRESS_LEFT_ODD_MAX
        FROM 
            ROAD_RANGING RD
            INNER JOIN ROAD_RANGE_LEFT RRL
            ON RD.PFI = RRL.PFI
        ''')

    logging.info('updating RIGHT')
    with conn.begin():
        conn.execute('''
        UPDATE RD SET
            RD.ADDRESS_RIGHT_MIN = RRR.ADDRESS_RIGHT_MIN,
            RD.ADDRESS_RIGHT_MAX = RRR.ADDRESS_RIGHT_MAX,
            RD.ADDRESS_RIGHT_ODD_MIN = RRR.ADDRESS_RIGHT_ODD_MIN,
            RD.ADDRESS_RIGHT_ODD_MAX = RRR.ADDRESS_RIGHT_ODD_MAX
        FROM 
            ROAD_RANGING RD
            INNER JOIN ROAD_RANGE_RIGHT RRR
            ON RD.PFI = RRR.PFI
        ''')

    logging.info('patching')
    with conn.begin():
        conn.execute('''
        -- MANUAL FIX FOR SPRINGVALE ROAD NUNAWADING ESTA CR 1024
        -- LOGGED ON DSE NES #7070. Status = pending @ 20090721
        UPDATE RD SET
            RD.ADDRESS_LEFT_MIN = 1,
            RD.ADDRESS_LEFT_MAX = 3,
            RD.ADDRESS_RIGHT_MIN = 14,
            RD.ADDRESS_RIGHT_MAX = 18,
            RD.ADDRESS_LEFT_ODD_MIN = 1,
            RD.ADDRESS_LEFT_ODD_MAX = 1,
            RD.ADDRESS_RIGHT_ODD_MIN = 0,
            RD.ADDRESS_RIGHT_ODD_MAX = 0
        FROM 
            ROAD_RANGING RD
        WHERE PFI = 5671261
        ''')

    logging.info('updating ADDRESS_TYPE')
    with conn.begin():
        conn.execute('''
        UPDATE ROAD_RANGING SET
            ADDRESS_TYPE = 
            CASE
            
            --LEFT SIDE IS THE SAME PARITY AND --RIGHT SIDE HAS NO VALUE
            WHEN 
                (ADDRESS_LEFT_ODD_MIN = ADDRESS_LEFT_ODD_MAX) AND 
                (ADDRESS_RIGHT_ODD_MIN IS NULL AND ADDRESS_RIGHT_ODD_MAX IS NULL) 
            THEN 0
            
            --RIGHT SIDE IS THE SAME PARITY AND --LEFT SIDE HAS NO VALUE
            WHEN
                (ADDRESS_RIGHT_ODD_MIN = ADDRESS_RIGHT_ODD_MAX) AND 
                (ADDRESS_LEFT_ODD_MIN IS NULL AND ADDRESS_LEFT_ODD_MAX IS NULL) 
            THEN 0

            -- LEFT AND RIGHT SIDE IS THE SAME PARITY AND DIFFERENT PARITY
            WHEN 
                (ADDRESS_LEFT_ODD_MIN = ADDRESS_LEFT_ODD_MAX) AND --LEFT SIDE IS THE SAME PARITY
                (ADDRESS_RIGHT_ODD_MIN = ADDRESS_RIGHT_ODD_MAX) AND --RIGHT SIDE IS THE SAME PARITY
                (ADDRESS_LEFT_ODD_MIN <> ADDRESS_RIGHT_ODD_MIN) -- LEFT AND RIGHT SIDE HAS DIFFERENT PARITY
            THEN 0

            -- NO ADDRESS DATA
            WHEN
                ADDRESS_LEFT_ODD_MIN IS NULL OR 
                ADDRESS_LEFT_ODD_MAX IS NULL OR
                ADDRESS_RIGHT_ODD_MIN IS NULL OR 
                ADDRESS_RIGHT_ODD_MAX IS NULL
            THEN 1

            -- ALL OTHER TYPES
            ELSE 1
            END
        ''')


def calc_road_flip_vicmap(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_sqlalchemy(em.server, em.database_name, init_geomtype='clr')

    logging.info('creating lmdb')
    if os.path.exists('c:\\temp\\road_flip_{}'.format(estamap_version)):
        shutil.rmtree('c:\\temp\\road_flip_{}'.format(estamap_version))
    env = lmdb.Environment(path='c:\\temp\\road_flip_{}'.format(estamap_version),
                           map_size=1000000000,
                           readonly=False,
                           max_dbs=10)
    arv_db = env.open_db('address_road_validation', dupsort=True)


    logging.info('dropping tables:')
    if dbpy.check_exists('ROAD_FLIP_VALIDATION_VICMAP', conn):
        logging.info('ROAD_FLIP_VALIDATION_VICMAP')
        conn.execute('drop table ROAD_FLIP_VALIDATION_VICMAP')
    if dbpy.check_exists('ROAD_FLIP_DATA_VICMAP', conn):
        logging.info('ROAD_FLIP_DATA_VICMAP')
        conn.execute('drop table ROAD_FLIP_DATA_VICMAP')

####    logging.info('reading ADDRESS_ROAD_VALIDATION')
####    ar_data = pd.read_sql('''
####    SELECT
####        A.HOUSE_NUMBER_1,
####        A.HOUSE_NUMBER_2,
####        AR.ADDR_PFI,
####        AR.ROAD_PFI,
####        AR.DIST_ALONG_ROAD,
####        AR.SIDE_OF_ROAD
####    FROM ADDRESS_ROAD_VALIDATION AR
####    INNER JOIN ADDRESS_VALIDATED_FINAL AF
####    ON AR.ADDR_PFI = AF.PFI
####    LEFT JOIN ADDRESS A
####    ON AF.PFI = A.PFI
####    ''', conn)
####    ar_data.set_index('ROAD_PFI', drop=False, inplace=True)
####    ar_data.sort_values(by=['DIST_ALONG_ROAD', 'HOUSE_NUMBER_2', 'HOUSE_NUMBER_1'],
####                        ascending=[True, False, False],
####                        inplace=True)
####    logging.info(len(ar_data))


    logging.info('loading into lmdb')
    with env.begin(write=True, db=arv_db) as txn:
        for enum, (hn1, hn2, addr_pfi, road_pfi, dist, side) in enumerate(conn.execute('''
            SELECT
                A.HOUSE_NUMBER_1,
                A.HOUSE_NUMBER_2,
                AR.ADDR_PFI,
                AR.ROAD_PFI,
                AR.DIST_ALONG_ROAD,
                AR.SIDE_OF_ROAD
            FROM ADDRESS_ROAD_VALIDATION AR
            INNER JOIN ADDRESS_VALIDATED_FINAL AF
            ON AR.ADDR_PFI = AF.PFI
            LEFT JOIN ADDRESS A
            ON AF.PFI = A.PFI
            ''')):
            txn.put(str(road_pfi), ','.join([str(hn1), # house number 1
                                             str(hn2), # house number 2
                                             str(addr_pfi), # address pfi
                                             str(road_pfi), # road pfi
                                             '{:.4f}'.format(dist), # distance along road
                                             str(side)])) # side of road
            if enum % 100000 == 0:
                logging.info(enum)
    logging.info(enum)


    logging.info('creating ROAD_FLIP_DATA_VICMAP')
    with conn.begin():
        conn.execute('''
        SELECT
            RR.PFI AS PFI,
            ISNULL(RR.ADDRESS_LEFT_MIN, -1) AS ADDRESS_LEFT_MIN,
            ISNULL(RR.ADDRESS_LEFT_MAX, -1) AS ADDRESS_LEFT_MAX,
            ISNULL(RR.ADDRESS_RIGHT_MIN, -1) AS ADDRESS_RIGHT_MIN,
            ISNULL(RR.ADDRESS_RIGHT_MAX, -1) AS ADDRESS_RIGHT_MAX,
            RR.ADDRESS_TYPE AS ADDRESS_TYPE,
            ISNULL(RX.FROM_NODE_ROAD_NAME_ID, -1) AS FROM_NODE_ROAD_NAME_ID,
            ISNULL(RX.TO_NODE_ROAD_NAME_ID, -1) AS TO_NODE_ROAD_NAME_ID

        INTO ROAD_FLIP_DATA_VICMAP

        FROM ROAD_RANGING RR
        LEFT JOIN ROAD_XSTREET RX
        ON RR.PFI = RX.PFI
        WHERE 
            (ADDRESS_LEFT_MIN IS NOT NULL OR
             ADDRESS_LEFT_MAX IS NOT NULL OR
             ADDRESS_RIGHT_MIN IS NOT NULL OR
             ADDRESS_RIGHT_MAX IS NOT NULL)
            AND ADDRESS_TYPE in (0,1)
        ORDER BY RR.PFI
        ''')

    logging.info('creating ROAD_FLIP_VALIDATION_VICMAP')
    conn.execute('''
    CREATE TABLE [dbo].[ROAD_FLIP_VALIDATION_VICMAP](
        [PFI] [int] NOT NULL,
        [LEFT_NUM_MIN] [int] NULL,
        [LEFT_NUM_MAX] [int] NULL,
        [LEFT_INVERSED] [int] NULL,
        [RIGHT_NUM_MIN] [int] NULL,
        [RIGHT_NUM_MAX] [int] NULL,
        [RIGHT_INVERSED] [int] NULL,
        [FLIP_STATUS] [int] NULL
    ) ON [PRIMARY]
    ''')

    logging.info('looping road flip data')
    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'ROAD_FLIP_VALIDATION_VICMAP') as sbc, \
         env.begin(db=arv_db) as arv_txn:

        arv_cursor = arv_txn.cursor()
        temp = [] 
        
        for enum, (road_pfi, left_min, left_max, right_min, right_max, addr_type, from_rnid, to_rnid) \
            in enumerate(conn.execute('''SELECT * FROM ROAD_FLIP_DATA_VICMAP''')):

            # load address road data
            left_data = []
            right_data = []                        
            arv_cursor.set_key(str(road_pfi))
            for road_data in arv_cursor.iternext_dup():
                hn1, hn2, addr_pfi, road_pfi, dist, side = road_data.split(',')
                hn1 = int(hn1)
                hn2 = int(hn2) if hn2 <> 'None' else -1
                addr_pfi = int(addr_pfi)
                road_pfi = int(road_pfi)
                dist = float(dist)
                side = str(side)

                if side == 'L':
                    left_data.append([hn1, hn2, addr_pfi, road_pfi, dist, side])
                else:
                    right_data.append([hn1, hn2, addr_pfi, road_pfi, dist, side])
            left_data = sorted(left_data, key=lambda x: x[4])  # sort by dist
            right_data = sorted(right_data, key=lambda x: x[4])  # sort by dist

            
            # determine LEFT
            if left_data:
                left_num_first = left_data[0][0]
                left_num_last = left_data[-1][0]
                left_same = left_num_first == left_num_last
                left_inversed = True if left_num_last < left_num_first else False
                
            else:
                left_data = None
                left_num_first = 0
                left_num_last = 0
                left_same = 1
                left_inversed = False

            # determine RIGHT
            if right_data:
                right_num_first = right_data[0][0]
                right_num_last = right_data[-1][0]
                right_same = right_num_first == right_num_last
                right_inversed = True if right_num_last < right_num_first else False
                
            else:
                right_data = None
                right_num_first = 0
                right_num_last = 0
                right_same = 1
                right_inversed = False

            # flip_status
            flip_status = int(left_inversed) + int(right_inversed)

            # not required to flip if not connected
            if flip_status == 1 and addr_type == 1:
                temp.append(road_pfi)
                if from_rnid == -1 or to_rnid == -1:
                    if to_rnid == -1 and \
                       (left_same is False and right_same is False) and \
                       (left_min is not None and right_min is not None):
                        flip_status = -1

            # load into db
            sbc.add_row((road_pfi,
                         left_min, left_max, left_inversed,
                         right_min, right_max, right_inversed,
                         flip_status))

            if enum % 10000 == 0:
                logging.info(enum)


def calc_road_flip_gnaf(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_sqlalchemy(em.server, em.database_name, init_geomtype='clr')

    logging.info('creating lmdb')
    if os.path.exists('c:\\temp\\road_flip_{}'.format(estamap_version)):
        shutil.rmtree('c:\\temp\\road_flip_{}'.format(estamap_version))
    env = lmdb.Environment(path='c:\\temp\\road_flip_{}'.format(estamap_version),
                           map_size=1000000000,
                           readonly=False,
                           max_dbs=10)
    arv_db = env.open_db('address_road_validation', dupsort=True)


    logging.info('dropping tables:')
    if dbpy.check_exists('ROAD_FLIP_VALIDATION_GNAF', conn):
        logging.info('ROAD_FLIP_VALIDATION_GNAF')
        conn.execute('drop table ROAD_FLIP_VALIDATION_GNAF')
    if dbpy.check_exists('ROAD_FLIP_DATA_GNAF', conn):
        logging.info('ROAD_FLIP_DATA_GNAF')
        conn.execute('drop table ROAD_FLIP_DATA_GNAF')

    logging.info('loading into lmdb')
    with env.begin(write=True, db=arv_db) as txn:
        for enum, (hn1, hn2, addr_pfi, road_pfi, dist, side) in enumerate(conn.execute('''
            SELECT
                A.HOUSE_NUMBER_1,
                A.HOUSE_NUMBER_2,
                AR.ADDR_PFI,
                AR.ROAD_PFI,
                AR.DIST_ALONG_ROAD,
                AR.SIDE_OF_ROAD
            FROM ADDRESS_ROAD_VALIDATION AR
            INNER JOIN ADDRESS_VALIDATED_FINAL AF
            ON AR.ADDR_PFI = AF.PFI
            LEFT JOIN ADDRESS A
            ON AF.PFI = A.PFI
            ''')):
            txn.put(str(road_pfi), ','.join([str(hn1), # house number 1
                                             str(hn2), # house number 2
                                             str(addr_pfi), # address pfi
                                             str(road_pfi), # road pfi
                                             '{:.4f}'.format(dist), # distance along road
                                             str(side)])) # side of road
            if enum % 100000 == 0:
                logging.info(enum)
    logging.info(enum)


    logging.info('creating ROAD_FLIP_DATA_VICMAP')
    with conn.begin():
        conn.execute('''
        SELECT
            RR.PFI AS PFI,
            ISNULL(RR.ADDRESS_LEFT_MIN, -1) AS ADDRESS_LEFT_MIN,
            ISNULL(RR.ADDRESS_LEFT_MAX, -1) AS ADDRESS_LEFT_MAX,
            ISNULL(RR.ADDRESS_RIGHT_MIN, -1) AS ADDRESS_RIGHT_MIN,
            ISNULL(RR.ADDRESS_RIGHT_MAX, -1) AS ADDRESS_RIGHT_MAX,
            RR.ADDRESS_TYPE AS ADDRESS_TYPE,
            ISNULL(RX.FROM_NODE_ROAD_NAME_ID, -1) AS FROM_NODE_ROAD_NAME_ID,
            ISNULL(RX.TO_NODE_ROAD_NAME_ID, -1) AS TO_NODE_ROAD_NAME_ID

        INTO ROAD_FLIP_DATA_VICMAP

        FROM ROAD_RANGING RR
        LEFT JOIN ROAD_XSTREET RX
        ON RR.PFI = RX.PFI
        WHERE 
            (ADDRESS_LEFT_MIN IS NOT NULL OR
             ADDRESS_LEFT_MAX IS NOT NULL OR
             ADDRESS_RIGHT_MIN IS NOT NULL OR
             ADDRESS_RIGHT_MAX IS NOT NULL)
            AND ADDRESS_TYPE in (0,1)
        ORDER BY RR.PFI
        ''')

    logging.info('creating ROAD_FLIP_VALIDATION_VICMAP')
    conn.execute('''
    CREATE TABLE [dbo].[ROAD_FLIP_VALIDATION_VICMAP](
        [PFI] [int] NOT NULL,
        [LEFT_NUM_MIN] [int] NULL,
        [LEFT_NUM_MAX] [int] NULL,
        [LEFT_INVERSED] [int] NULL,
        [RIGHT_NUM_MIN] [int] NULL,
        [RIGHT_NUM_MAX] [int] NULL,
        [RIGHT_INVERSED] [int] NULL,
        [FLIP_STATUS] [int] NULL
    ) ON [PRIMARY]
    ''')

    logging.info('looping road flip data')
    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'ROAD_FLIP_VALIDATION_VICMAP') as sbc, \
         env.begin(db=arv_db) as arv_txn:

        arv_cursor = arv_txn.cursor()
        temp = [] 
        
        for enum, (road_pfi, left_min, left_max, right_min, right_max, addr_type, from_rnid, to_rnid) \
            in enumerate(conn.execute('''SELECT * FROM ROAD_FLIP_DATA_VICMAP''')):

            # load address road data
            left_data = []
            right_data = []                        
            arv_cursor.set_key(str(road_pfi))
            for road_data in arv_cursor.iternext_dup():
                hn1, hn2, addr_pfi, road_pfi, dist, side = road_data.split(',')
                hn1 = int(hn1)
                hn2 = int(hn2) if hn2 <> 'None' else -1
                addr_pfi = addr_pfi
                road_pfi = int(road_pfi)
                dist = float(dist)
                side = str(side)

                if side == 'L':
                    left_data.append([hn1, hn2, addr_pfi, road_pfi, dist, side])
                else:
                    right_data.append([hn1, hn2, addr_pfi, road_pfi, dist, side])
            left_data = sorted(left_data, key=lambda x: x[4])  # sort by dist
            right_data = sorted(right_data, key=lambda x: x[4])  # sort by dist

            
            # determine LEFT
            if left_data:
                left_num_first = left_data[0][0]
                left_num_last = left_data[-1][0]
                left_same = left_num_first == left_num_last
                left_inversed = True if left_num_last < left_num_first else False
                
            else:
                left_data = None
                left_num_first = 0
                left_num_last = 0
                left_same = 1
                left_inversed = False

            # determine RIGHT
            if right_data:
                right_num_first = right_data[0][0]
                right_num_last = right_data[-1][0]
                right_same = right_num_first == right_num_last
                right_inversed = True if right_num_last < right_num_first else False
                
            else:
                right_data = None
                right_num_first = 0
                right_num_last = 0
                right_same = 1
                right_inversed = False

            # flip_status
            flip_status = int(left_inversed) + int(right_inversed)

            # not required to flip if not connected
            if flip_status == 1 and addr_type == 1:
                temp.append(road_pfi)
                if from_rnid == -1 or to_rnid == -1:
                    if to_rnid == -1 and \
                       (left_same is False and right_same is False) and \
                       (left_min is not None and right_min is not None):
                        flip_status = -1

            # load into db
            sbc.add_row((road_pfi,
                         left_min, left_max, left_inversed,
                         right_min, right_max, right_inversed,
                         flip_status))

            if enum % 10000 == 0:
                logging.info(enum)

def calc_address_components(estamap_version):
    
    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_sqlalchemy(em.server, em.database_name, init_geomtype='clr')

    logging.info('dropping tables:')
    if dbpy.check_exists('ADDRESS_COMPONENTS', conn):
        logging.info('ADDRESS_COMPONENTS')
        conn.execute('drop table ADDRESS_COMPONENTS')

    logging.info('creating ADDRESS_COMPONENTS')
    with conn.begin():
        conn.execute('''
        CREATE TABLE [dbo].[ADDRESS_COMPONENTS](
            [PFI] [nvarchar](15) NOT NULL,

            -- LV_APT
            [BLG_UNIT_ID_1] [int] NULL,
            [BLG_UNIT_SUFFIX_1] [nvarchar](2) NULL,
            [FLOOR_NO_1] [int] NULL,
            [LV_APT] [nvarchar](5) NULL,

            -- ST_NUM
            [HOUSE_PREFIX_1] [nvarchar](2) NULL,
            [HOUSE_NUMBER_1] [int] NULL,
            [HOUSE_SUFFIX_1] [nvarchar](2) NULL,
            [ST_NUM] [nvarchar](11) NULL,

            -- HI_NUM
            [HOUSE_PREFIX_2] [nvarchar](2) NULL,
            [HOUSE_NUMBER_2] [int] NULL,
            [HOUSE_SUFFIX_2] [nvarchar](2) NULL,
            [HI_NUM] [nvarchar](5) NULL,

            -- ADDRESS_STRING
            [ROAD_NAME] [nvarchar](45) NULL,
            [ROAD_TYPE] [nvarchar](15) NULL,
            [ROAD_SUFFIX] [nvarchar](2) NULL,
            [LOCALITY_NAME] [nvarchar](255) NULL,
            [ADDRESS_STRING] [nvarchar](255) NULL
                                    
        ) ON [PRIMARY]
        ''')

    logging.info('insert into ADDRESS_COMPONENTS')
    with conn.begin():
        conn.execute('''
        INSERT INTO ADDRESS_COMPONENTS
            (PFI,
            BLG_UNIT_ID_1,
            BLG_UNIT_SUFFIX_1,
            FLOOR_NO_1,
            LV_APT,
            HOUSE_PREFIX_1,
            HOUSE_NUMBER_1,
            HOUSE_SUFFIX_1,
            ST_NUM,
            HOUSE_PREFIX_2,
            HOUSE_NUMBER_2,
            HOUSE_SUFFIX_2,
            HI_NUM,
            ROAD_NAME,
            ROAD_TYPE,
            ROAD_SUFFIX,
            LOCALITY_NAME,
            ADDRESS_STRING
            )
        SELECT
            A.PFI,

            -- LV_APT
            A.BLG_UNIT_ID_1,
            A.BLG_UNIT_SUFFIX_1,
            A.FLOOR_NO_1,
            CAST(ISNULL(CAST(A.BLG_UNIT_ID_1 AS VARCHAR)+ ISNULL(CAST(A.BLG_UNIT_SUFFIX_1 AS VARCHAR),''), A.FLOOR_NO_1) AS VARCHAR(5)) AS LV_APT,

            -- ST_NUM
            A.HOUSE_PREFIX_1,
            A.HOUSE_NUMBER_1,
            A.HOUSE_SUFFIX_1,
            ISNULL(A.HOUSE_PREFIX_1,'') + CAST(A.HOUSE_NUMBER_1 AS VARCHAR(11)) + ISNULL(A.HOUSE_SUFFIX_1,'') AS ST_NUM,

            A.HOUSE_PREFIX_2,
            A.HOUSE_NUMBER_2,
            A.HOUSE_SUFFIX_2,
            ISNULL(A.HOUSE_PREFIX_2,'') + CAST(A.HOUSE_NUMBER_2 AS VARCHAR(11)) + ISNULL(HOUSE_SUFFIX_2,'') as HI_NUM,
            

            -- ADDRESS_STRING
            RN.ROAD_NAME,
            RN.ROAD_TYPE,
            RN.ROAD_SUFFIX,
            AD.LOCALITY_NAME,
            ISNULL(AD.LOCALITY_NAME,'') + '_' + ISNULL(RN.ROAD_NAME,'') + '_' + ISNULL(RN.ROAD_TYPE,'') + '_' + ISNULL(RN.ROAD_SUFFIX,'') + '_' +
                ISNULL(A.HOUSE_PREFIX_1,'') + CAST(A.HOUSE_NUMBER_1 AS VARCHAR(11)) + ISNULL(A.HOUSE_SUFFIX_1,'') + '_' AS ADDRESS_STRING
        FROM ADDRESS A
        LEFT JOIN ADDRESS_RNID AI
        ON A.PFI = AI.PFI
        LEFT JOIN ROAD_NAME_REGISTER RN
        ON AI.ROAD_NAME_ID = RN.ROAD_NAME_ID
        LEFT JOIN ADDRESS_DETAIL AD
        ON A.PFI = AD.PFI                         
        ''')

    logging.info('creating index on ADDRESS_COMPONENTS')
    with conn.begin():
        conn.execute('''
        CREATE UNIQUE CLUSTERED INDEX [IX_ADDRESS_COMPONENTS_PFI] ON [dbo].[ADDRESS_COMPONENTS]
        (
            [PFI] ASC
        )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
        ''')

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
                ###########

                calc_address_components(estamap_version)
##                address_validation_phase_1(estamap_version)
##                calc_road_ranges_phase_1(estamap_version)
##                address_validation_phase_2(estamap_version)
##                export_address_validated(estamap_version)
##                register_new_address(estamap_version)
##                calc_road_ranges(estamap_version)
##                calc_road_flip_vicmap(estamap_version)
                

                # ----------
                
                            

                ###########   
            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')

