
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import inspect
import urllib
import pandas as pd
import re
import inspect as ins
import os, sys


def engine_creation(creds, serverName, dbType, dbName):
    try:
        # dialect+driver://username:password@host:port/database
        # http://docs.sqlalchemy.org/en/latest/core/engines.html
        userName = creds[serverName]['userName']
        port = creds[serverName]['port']
        passWord = creds[serverName]['passWord']

        if dbType == 'postgres':
            db_engine = create_engine('postgresql://%s:%s@%s:%s/%s' % (userName, passWord, serverName, port, dbName))
            return db_engine

        elif dbType == 'oracle':
            # oracle+cx_oracle://user:pass@host:port/dbname[?key=value&key=value...]
            db_engine = create_engine(
                'oracle+cx_oracle://%s:%s@%s:%s/%s' % (userName, passWord, serverName, port, dbName))
            return db_engine

        elif dbType == 'mssql':
            if 'driver' in creds[serverName].keys():
                driver = creds[serverName]['driver']
                quoted = urllib.quote_plus(
                    'DRIVER={' + driver + '};SERVER=' + serverName + ';UID=' + userName + ';PWD=' + passWord + ';PORT='
                    + port + '')
                db_engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
                return db_engine
            else:
                print "Check the creds is valid or missing and re-run"

    except Exception as e:
        print e
        print ins.stack()[0][3]
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)


def create_datasource(testcase_id, test_data, dict_creds):
    try:
        tc_id_data = verify_data(test_data, testcase_id)

        if tc_id_data['sourcedb'] != '' and tc_id_data['sourceServer'] != '' and tc_id_data['sourcedbType'] != '':

            source_engine = engine_creation(dict_creds, tc_id_data['sourceServer'], tc_id_data['sourcedbType'],
                                            tc_id_data['sourcedb'])

            print "-----------------SOURCE ENGINE CREATED-----------------"
            print '{}'.format(source_engine)
            print "-----------------SOURCE ENGINE CREATED-----------------"

            source_df, source_meta = create_dataframe(source_engine, tc_id_data['sourcePrimaryKey'],
                                                      tc_id_data['sourceTable'], tc_id_data['querySource'])

        else:
            print '{}|{}|{}'.format(tc_id_data['sourcedb'], tc_id_data['sourceServer'], tc_id_data['sourcedbType'])
            print '-------Verify Above Data and Retry--------'

        if tc_id_data['targetdb'] != '' and tc_id_data['targetServer'] != '' and tc_id_data['targetdbType'] != '':

            target_engine = engine_creation(dict_creds, tc_id_data['targetServer'], tc_id_data['targetdbType'],
                                            tc_id_data['targetdb'])

            print "-----------------TARGET ENGINE CREATED-----------------"
            print '{}'.format(target_engine)
            print "-----------------TARGET ENGINE CREATED-----------------"

            target_df, target_meta = create_dataframe(target_engine, tc_id_data['targetPrimaryKey'],
                                                      tc_id_data['targetTable'], tc_id_data['queryTarget'])

        else:
            print '{}|{}|{}'.format(tc_id_data['targetdb'], tc_id_data['targetServer'], tc_id_data['targetdbType'])
            print '-------Verify Above Data and Retry--------'

        return source_df, target_df, source_meta, target_meta, tc_id_data

    except Exception as e:
        print e
        print ins.stack()[0][3]
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)


def create_dataframe(engine, primaryKey, targetTable, logic=''):
    print '*********Creating Data frame for Mentioned Parameters*********'
    print '{}|Target Table: {}|Primary Key :{}|Business Logic:{}'.format(engine, targetTable, primaryKey, logic)
    print '*********Creating Data frame for Mentioned Parameters*********'
    # Query Database Table and return a DataFrame
    try:
        if logic != '':
            insp = inspect(engine)
            targetTable = re.split('\\.', targetTable)
            if len(targetTable) == 1:
                return_df = pd.read_sql("%s" % logic, con=engine)
                ddl_dict = insp.get_columns(targetTable[0])
            else:
                meta = MetaData()
                # meta.reflect(bind=engine, schema=targetTable[0])

                table = Table(str(targetTable[1]), meta, autoload=True, autoload_with=engine, schema=targetTable[0])
                insp = inspect(engine)
                table_check = engine.has_table(targetTable[1], schema=targetTable[0])
                if table_check:
                    print "##########################Table Exists :{}".format(targetTable[1])
                    ddl_dict = insp.get_columns(targetTable[1])
                    return_df = pd.read_sql("%s" % logic, con=engine)
                else:
                    for key, value in meta.tables.iteritems():
                        table_name = meta.tables[key]
                        print table_name
                    print "##########################Table Not Found in Database : {}".format(targetTable[0])

            print "Logic Applied on Above Table {}".format(logic)
            return return_df, ddl_dict

        else:
            targetTable = re.split('\\.', targetTable)
            if len(targetTable) == 1:
                meta = MetaData()
                # meta.reflect(bind=engine, schema='public')
                insp = inspect(engine)
                table = Table(str(targetTable[0]), meta, autoload=True, autoload_with=engine)
                table_check = engine.has_table(targetTable[0])
                if table_check:
                    print "###########################Table Exists :{}".format(targetTable[0])
                    ddl_dict = insp.get_columns(targetTable[0])
                    return_df = pd.read_sql("SELECT * FROM %s ORDER BY %s ASC;" % (targetTable[0], primaryKey), con=engine)
                else:
                    for key, value in meta.tables.iteritems():
                        table_name = meta.tables[key]
                        print table_name
                    print "###########################Table Not Found in Database :{}".format(targetTable[0])
            else:
                meta = MetaData()
                # meta.reflect(bind=engine, schema=targetTable[0])
                insp = inspect(engine)
                table = Table(str(targetTable[1]), meta, autoload=True, autoload_with=engine, schema=targetTable[0])
                table_check = engine.has_table(targetTable[1], schema=targetTable[0])
                if table_check:
                    print "###########################Table Exists :{}".format(targetTable[1])
                    ddl_dict = insp.get_columns(targetTable[1], schema=targetTable[0])
                    return_df = pd.read_sql(
                        "SELECT * FROM %s.%s ORDER BY %s ASC;" % (targetTable[0], targetTable[1], primaryKey), con=engine)
                else:
                    for key, value in meta.tables.iteritems():
                        table_name = meta.tables[key]
                        print table_name
                    print "##############################Table Not Found in Database : {} ".format(targetTable[1])

            return return_df, ddl_dict
    except Exception as e:
        print e
        print ins.stack()[0][3]
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)


def verify_data(test_data, testcase_id):


    if test_data[testcase_id]:
        tc_id_data = test_data[testcase_id]
    else:
        print test_data[testcase_id]
        print "TEST DATA NOT AVAILABLE FOR : {}".format(testcase_id)
        return
    key_list = ['sourcePrimaryKey', 'sourcedbType', 'sourcedb', 'sourceServer', 'sourceTable', 'sourceColumn',
                'targetPrimaryKey', 'targetdbType', 'targetdb', 'targetServer', 'targetTable', 'targetColumn',
                'testClass', 'queryTarget', 'querySource', 'excludeColumns']
    print '----------TEST DATA STARTS: {} ----------'.format(testcase_id)

    for key in key_list:
        if tc_id_data.get(key):
            print '{}={}'.format(key, tc_id_data[key])
        else:
            tc_id_data[key] = ''
            print '{}={}'.format(key, tc_id_data[key])
    print '----------TEST DATA ENDS: {} -----------'.format(testcase_id)
    return tc_id_data


