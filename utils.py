# -*- coding: utf8 -*-
# Firebird
# Microsoft SQL Server
# MySQL
# Oracle
# PostgreSQL
# SQLite
# Sybase
# ibm_db_sa - driver for IBM DB2 and Informix, developed jointly by IBM and SQLAlchemy developers.
# sqlalchemy-redshift - driver for Amazon Redshift, adapts the existing PostgreSQL/psycopg2 driver.
# sqlalchemy_exasol - driver for EXASolution.
# sqlalchemy-sqlany - driver for SAP Sybase SQL Anywhere, developed by SAP.
# sqlalchemy-monetdb - driver for MonetDB.
# snowflake-sqlalchemy - driver for Snowflake.
# sqlalchemy-tds - driver for MS-SQL, on top of pythone-tds.
# crate - driver for CrateDB.

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import inspect
import wx
from os import path
import re
from openpyxl import load_workbook
from openpyxl.styles.differential import DifferentialStyle
from openpyxl.styles import Color, PatternFill, Font, Border
from openpyxl.formatting.rule import Rule
import datetime
import urllib

class App():

    def __init__(self):
        # self.pysql = lambda q: pdsql.sqldf(q, globals())

        self.pathname = self.open_filedialog()
        self.df_testcase, self.selected_sheet = self.read_testcase(self.pathname)
        test_selected = self.select_testcases(self.df_testcase)
        self.dict_creds = self.read_creds()
        self.test_data = self.get_testdata(test_selected)
        self.testclass_selection(self.test_data)

    def testclass_selection(self, test_data):
        try:

            for key, value in test_data.iteritems():
                self.testcase_id = key
                self.source_df, self.target_df, self.source_meta, self.target_meta = self.create_datasource(self.testcase_id)
                if self.testClass == 'DataValidation' or self.testClass == 'Data Validation':
                    # if self.df_comparison(self.testcase_id):
                    if self.targetColumn != '':
                        targetColumn = re.split('\[|\]|\,|\\\'|\\"', self.targetColumn)
                        self.targetColumn = [x for x in targetColumn if x]
                        if self.onebyone_check(self.testcase_id, self.targetColumn):
                            if self.update_result(self.testcase_id):
                                print "{} Executed and Results as PASS Updated ".format(self.testcase_id)
                            else:
                                print "{} Executed and Results as FAIL Updated ".format(self.testcase_id)
                        else:
                            self.update_result(self.testcase_id, override='FAIL')
                            print "{} Something went Wrong, Results as FAIL Updated ".format(self.testcase_id)

                    else:
                        self.targetColumn = self.select_columns(self.excludeColumns, self.testClass)
                        if self.onebyone_check(self.testcase_id, self.targetColumn):
                            if self.update_result(self.testcase_id):
                                print "{} Executed and Results as PASS Updated ".format(self.testcase_id)
                            else:
                                print "{} Executed and Results as FAIL Updated ".format(self.testcase_id)
                        else:
                            self.update_result(self.testcase_id, override='FAIL')
                            print "{} Something went Wrong, Results as FAIL Updated ".format(self.testcase_id)

                elif self.testClass == 'CountCheck' or self.testClass == 'Count Check':
                    if self.check_count(self.testcase_id):
                        if self.update_result(self.testcase_id):
                            print "{} Executed and Results as PASS Updated ".format(self.testcase_id)
                        else:
                            print "{} Executed and Results as FAIL Updated ".format(self.testcase_id)

                elif self.testClass == 'DuplicateCheck' or self.testClass == 'Duplicate Check':

                    # column names as a list to verify duplicates on each columns
                    if self.targetColumn != '':
                        targetColumn = re.split('\[|\]|\,|\\\'|\\"', self.targetColumn)
                        self.targetColumn = [x for x in targetColumn if x]
                        if self.check_duplicates(self.targetColumn, self.testcase_id, 'target'):
                            if self.update_result(self.testcase_id, override='PASS'):
                                print "{} Executed and Results as PASS Updated ".format(self.testcase_id)
                        else:
                            self.update_result(self.testcase_id, override='FAIL')
                            print "{} Executed and Results as FAIL Updated ".format(self.testcase_id)
                    else:
                        self.targetColumn = self.select_columns(self.excludeColumns, self.testClass)
                        if self.check_duplicates(self.targetColumn, self.testcase_id, 'target'):
                            if self.update_result(self.testcase_id, override='PASS'):
                                print "{} Executed and Results as PASS Updated ".format(self.testcase_id)
                        else:
                            self.update_result(self.testcase_id, override='FAIL')
                            print "{} Executed and Results as FAIL Updated ".format(self.testcase_id)

                elif self.testClass == 'NullCheck' or self.testClass == 'Null Check':

                    if self.targetColumn != '':
                        targetColumn = re.split('\[|\]|\,|\\\'|\\"', self.targetColumn)
                        self.targetColumn = [x for x in targetColumn if x]
                        if self.check_null(self.targetColumn, self.testcase_id):
                            if self.update_result(self.testcase_id, override='PASS'):
                                print "{} Executed and Results as PASS Updated ".format(self.testcase_id)
                            else:
                                print "{} Executed and Results as FAIL Updated ".format(self.testcase_id)
                        else:
                            self.update_result(self.testcase_id, override='FAIL')
                            print "{} Executed and Results as FAIL Updated ".format(self.testcase_id)
                    else:
                        self.targetColumn = self.select_columns(self.excludeColumns,self.testClass)
                        if self.check_null(self.targetColumn, self.testcase_id):
                            if self.update_result(self.testcase_id, override='PASS'):
                                print "{} Executed and Results as PASS Updated ".format(self.testcase_id)
                            else:
                                print "{} Executed and Results as FAIL Updated ".format(self.testcase_id)
                        else:
                            self.update_result(self.testcase_id, override='FAIL')
                            print "{} Executed and Results as FAIL Updated ".format(self.testcase_id)

                elif self.testClass == 'DDLCheck' or self.testClass == 'DDL Check':
                    self.check_metadata(self.testcase_id)
                    self.update_result(self.testcase_id)


                elif self.testClass == 'LoadStrategy':
                    print "Not Implemented LoadStrategy"
                else:
                    print "Please Add a Test Class for framework to know how to Execute Case"
        except Exception as e:
            print e

    def open_filedialog(self):
        try:
            app = wx.App()

            frame = wx.Frame(None, -1, 'Select XLSX File')
            frame.SetSize(0,0,200,50)
            # ask the user what new file to open
            with wx.FileDialog(frame, "Open XLSX file", wildcard="XLSX files (*.xlsx)|*.xlsx",
                               style=wx.FD_OPEN | wx.FD_FILE_MUST_EXIST) as fileDialog:

                if fileDialog.ShowModal() == wx.ID_CANCEL:
                    wx.LogError("No Files Selected")
                    return  # the user changed their mind

                # Proceed loading the file chosen by the user
                pathname = fileDialog.GetPath()
                try:
                    with open(pathname, 'r') as file:
                        print ("Selected Path : '%s'" % pathname)
                        return pathname

                except IOError:
                    wx.LogError("Cannot open file '%s'." % file)
        except Exception as e:
            app.Destroy()
            print e

    def select_testcases(self, df_testcases):
        app = wx.App()
        df = pd.DataFrame()
        frame = wx.Frame(None, -1, 'Select Tests')
        frame.SetSize(0, 0, 200, 50)

        sel = wx.Button(frame, -1, 'Select All', size=(100, -1))
        des = wx.Button(frame, -1, 'Deselect All', size=(100, -1))

        df['Test Case'] = df_testcases['Test Case ID'].astype(str) + '_' + df_testcases['Test Class']
        test_list = df['Test Case']

        frame.Bind(wx.EVT_BUTTON, self.OnSelectAll, id=sel.GetId())
        frame.Bind(wx.EVT_BUTTON, self.OnDeselectAll, id=des.GetId())

        dlg = wx.MultiChoiceDialog(frame,
                                   "Pick The TestCases To Be Executed",
                                   "TestPandas", test_list)

        if dlg.ShowModal() == wx.ID_OK:
            selections = dlg.GetSelections()
            strings = [test_list[x] for x in selections]
            print "You chose TestCases:" + str(strings)

            self.create_results_sheet(strings)
            print selections
            return selections

    def select_sheet(self, sheet_list):
        app = wx.App()

        frame = wx.Frame(None, -1, 'Select Sheet For Test Cases')
        frame.SetSize(0, 0, 200, 50)

        dlg = wx.SingleChoiceDialog(frame,
                                   "Pick The SheetName To View TestCases",
                                   "TestPandas", sheet_list)

        if (dlg.ShowModal() == wx.ID_OK):
            strings = dlg.GetStringSelection()

            #strings = [sheet_list[x] for x in selections]
            print "You choose SheetName:" + str(strings)
            return strings

    def create_datasource(self, testcase_id):
        try:
            print self.test_data[testcase_id]
            if self.test_data[testcase_id]:
                if 'sourcePrimaryKey' in self.test_data[testcase_id]:
                    self.sourcePrimaryKey = self.test_data[testcase_id]['sourcePrimaryKey']
                else:
                    self.sourcePrimaryKey = ''
                if 'sourcedbType' in self.test_data[testcase_id]:
                    self.sourcedbType = self.test_data[testcase_id]['sourcedbType']
                else:
                    self.sourcedbType = ''
                if 'sourcedb' in self.test_data[testcase_id]:
                    self.sourcedb = self.test_data[testcase_id]['sourcedb']
                else:
                    self.sourcedb = ''
                if 'sourceServer' in self.test_data[testcase_id]:
                    self.sourceServer = self.test_data[testcase_id]['sourceServer']
                else:
                    self.sourceServer = ''
                if 'sourceTable' in self.test_data[testcase_id]:
                    self.sourceTable = self.test_data[testcase_id]['sourceTable']
                else:
                    self.sourceTable = ''

                self.targetPrimaryKey = self.test_data[testcase_id]['targetPrimaryKey']
                self.targetdbType = self.test_data[testcase_id]['targetdbType']
                self.targetdb = self.test_data[testcase_id]['targetdb']
                self.targetServer = self.test_data[testcase_id]['targetServer']
                self.targetTable = self.test_data[testcase_id]['targetTable']
                self.testClass = self.test_data[testcase_id]['testClass']
                if 'sourceColumn' in self.test_data[testcase_id]:
                    self.sourceColumn = self.test_data[testcase_id]['sourceColumn']
                else:
                    self.sourceColumn = ''

                if 'targetColumn' in self.test_data[testcase_id]:
                    self.targetColumn = self.test_data[testcase_id]['targetColumn']
                else:
                    self.targetColumn = ''

                if 'queryTarget' in self.test_data[testcase_id]:
                    self.targetQuery = self.test_data[testcase_id]['queryTarget']
                else:
                    self.targetQuery = ''

                if 'querySource' in self.test_data[testcase_id]:
                    self.sourceQuery = self.test_data[testcase_id]['querySource']
                else:
                    self.sourceQuery = ''

                if 'excludeColumns' in self.test_data[testcase_id]:
                    self.excludeColumns = self.test_data[testcase_id]['excludeColumns']
                else:
                    self.excludeColumns = ''

                if self.sourcedb != '' and self.sourceServer != '' and self.sourcedbType != '':

                    source_engine = self.create_engine(self.dict_creds, self.sourceServer, self.sourcedbType, self.sourcedb)
                    source_df, source_meta = self.create_dataframe(source_engine, self.sourcePrimaryKey, self.sourceTable, self.sourceQuery)

                if self.targetdb != '' and self.targetServer != '' and self.targetdbType != '':

                    target_engine = self.create_engine(self.dict_creds, self.targetServer, self.targetdbType, self.targetdb)
                    target_df, target_meta = self.create_dataframe(target_engine, self.targetPrimaryKey, self.targetTable, self.targetQuery)

                return source_df, target_df, source_meta, target_meta

            else:
                print "TestCase ID not matched/ Not Found"


        except Exception as e:
            print e

    def select_columns(self, excludeColumns, testclass):
        if excludeColumns != '':
            column_list = list(self.target_df.columns.values)

            excludeColumns = re.split('\[|\]|\,|\\\'|\\"',excludeColumns)
            excludeColumns = [x for x in excludeColumns if x]

            print "Columns Excluded :{}".format(excludeColumns)
            for column in excludeColumns:
                if column in column_list:
                    column_list.remove(column)
                else:
                    print "Column Not Exists To Be Exclude :{}".format(column)
            return column_list
        else:
            app = wx.App()

            frame = wx.Frame(None, -1, 'Select Tests')
            frame.SetSize(0, 0, 200, 50)

            column_list = list(self.target_df.columns.values)
            dlg = wx.MultiChoiceDialog(frame,
                                       "Pick The Columns For {}".format(testclass),
                                       "Aciom", column_list)

            if (dlg.ShowModal() == wx.ID_OK):
                selections = dlg.GetSelections()
                strings = [column_list[x] for x in selections]
                print "You chose:" + str(strings)
                return strings

    def read_testcase(self, pathname):
        try:
            pd.set_option('max_colwidth', 1024)
            if pathname != '':
                df_excel_sheet = pd.ExcelFile(pathname)
                sheet_list = df_excel_sheet.sheet_names
                selected_sheet = self.select_sheet(sheet_list)
                # selected_sheet = selected_sheet[0].encode('utf8')
                df_excel = pd.read_excel(pathname,sheet_name=selected_sheet)
            else:
                print 'No Pathname Available , Retry Selecting the File'
            # print df_excel
            return df_excel, selected_sheet
        except IOError:
            wx.LogError("Cannot open file '%s'." % file)

    def create_engine(self, creds, serverName, dbType, dbName):
        try:
        # dialect+driver://username:password@host:port/database
        # http://docs.sqlalchemy.org/en/latest/core/engines.html
            userName = creds[serverName]['userName']
            port = creds[serverName]['port']
            passWord = creds[serverName]['passWord']

            if dbType == 'postgres':
                db_engine = create_engine('postgresql://%s:%s@%s:%s/%s'%(userName, passWord, serverName, port, dbName))
                return db_engine

            elif dbType == 'oracle':
                # oracle+cx_oracle://user:pass@host:port/dbname[?key=value&key=value...]
                db_engine = create_engine('oracle+cx_oracle://%s:%s@%s:%s/%s' % (userName, passWord, serverName, port, dbName))
                return db_engine

            elif dbType == 'mssql':
                if 'driver' in creds[serverName].keys():
                    driver = creds[serverName]['driver']
                    quoted = urllib.quote_plus('DRIVER={'+driver+'};SERVER='+serverName+';UID='+userName+';PWD='+passWord+';PORT='+port+'')
                    db_engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
                    return db_engine
                else:
                    print "Check the creds is valid or missing and re-run"

        except Exception as e:
            print e

    def create_dataframe(self, engine, primaryKey, targetTable, logic=''):
        print engine
        # Query Database Table and return a DataFrame
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
                    print "Table Exists :{}".format(targetTable[1])
                    ddl_dict = insp.get_columns(targetTable[1])
                    return_df = pd.read_sql("%s" % logic, con=engine)
                else:
                    for key, value in meta.tables.iteritems():
                        table_name = meta.tables[key]
                        print table_name
                    print "Table Not Found in Database : {}".format(targetTable[0])

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
                    print "Table Exists :{}".format(targetTable[0])
                    ddl_dict = insp.get_columns(targetTable[0])
                    return_df = pd.read_sql("SELECT * FROM %s ORDER BY %s ASC;" % (targetTable[0], primaryKey), con=engine)
                else:
                    for key, value in meta.tables.iteritems():
                        table_name = meta.tables[key]
                        print table_name
                    print "Table Not Found in Database :{}".format(targetTable[0])
            else:
                meta = MetaData()
                # meta.reflect(bind=engine, schema=targetTable[0])
                insp = inspect(engine)
                table = Table(str(targetTable[1]), meta, autoload=True, autoload_with=engine, schema=targetTable[0])
                table_check = engine.has_table(targetTable[1], schema=targetTable[0])
                if table_check:
                    print "Table Exists :{}".format(targetTable[1])
                    ddl_dict = insp.get_columns(targetTable[1], schema=targetTable[0])
                    return_df = pd.read_sql("SELECT * FROM %s.%s ORDER BY %s ASC;" % (targetTable[0], targetTable[1], primaryKey), con=engine)
                else:
                    for key, value in meta.tables.iteritems():
                        table_name = meta.tables[key]
                        print table_name
                    print "Table Not Found in Database : {} ".format(targetTable[1])

            return return_df, ddl_dict

    def check_duplicates(self, column_name, testcase_id, source_name):
        column_name = [x.encode('utf-8') for x in column_name ]
        print "Duplicate Check on Columns : {}".format(column_name)
        # column_name = re.sub('\'|\[|\]|\"','',column_name)
        # column_name = re.split(',',column_name)
        max_index = 1
        check_len = 0
        if source_name == 'target':
            book = load_workbook(self.pathname)
            sheet = book.get_sheet_by_name(str(testcase_id))
            sheet['A1'].value = 'Duplicate Check'
            # for column in column_name:
            #     print '{}_Duplicates Check on {}'.format(testcase_id,column)
            #     df_dupes = self.target_df[self.target_df.duplicated(subset=column_name,keep=False)]
            #     if column != '':
            #         writer = pd.ExcelWriter(self.pathname)
            #         writer.book = book
            #         writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
            #         # Save the changes to excel but only include the columns changed
            #         cell = sheet['A{}'.format(max_index)]
            #         cell.font = cell.font.copy(bold=True)
            #         sheet['A{}'.format(max_index + 1)].value = column
            #         df_dupes.to_excel(writer, sheet_name=str(testcase_id),startrow=max_index+2)
            #         max_index = sheet.max_row
            #         check_len =+ len(df_dupes)
            if len(column_name) != 0:
                df_dupes = self.target_df[self.target_df.duplicated(subset=column_name, keep=False)]
                writer = pd.ExcelWriter(self.pathname)
                writer.book = book
                writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
                for column in df_dupes:
                    if column in column_name:
                        continue
                    else:
                        df_dupes = df_dupes.drop(column,axis=1)
                df_dupes.to_excel(writer, sheet_name=str(testcase_id),startrow=max_index+2)
                max_index = sheet.max_row
                check_len =+ len(df_dupes)

            # Make the text of the cell bold and italic
            cell2 = sheet['A{}'.format(max_index + 1)]
            cell2.font = cell2.font.copy(bold=True)
            sheet['A{}'.format(max_index + 1)].value = 'Execution TimeStamp'
            sheet['B{}'.format(max_index + 1)].value = datetime.datetime.now()
            writer.save()
            print check_len
            if check_len == 0:
                return True
            else:
                return False
        else:
            print 'To be Checked in Target DB , Enter the Source Name Correctly !!'
            return False

    def check_count(self, testcase_id):

        source_column = len(self.source_df.columns.values.tolist())
        source_row = len(self.source_df.index.values.tolist())
        target_column = len(self.target_df.columns.values.tolist())
        target_row = len(self.target_df.index.values.tolist())

        book = load_workbook(self.pathname)
        writer = pd.ExcelWriter(self.pathname)
        writer.book = book
        sheet = book.get_sheet_by_name(str(testcase_id))
        max_index = sheet.max_row
        # Make the text of the cell bold and italic
        # cell = sheet['A{}'.format(max_index + 1)]
        sheet['A{}'.format(max_index + 2)].value = 'Column'
        sheet['A{}'.format(max_index + 3)].value = 'Row'
        sheet['A{}'.format(max_index + 4)].value = 'Execution TimeStamp'
        sheet['B{}'.format(max_index + 1)].value = 'Source'
        sheet['C{}'.format(max_index + 1)].value = 'Target'
        sheet['D{}'.format(max_index + 1)].value = 'Result'

        sheet['B{}'.format(max_index + 2)].value = source_column
        sheet['C{}'.format(max_index + 2)].value = target_column
        sheet['B{}'.format(max_index + 3)].value = source_row
        sheet['C{}'.format(max_index + 3)].value = target_row

        if source_column == target_column:
            sheet['D{}'.format(max_index + 2)].value = 'PASS'
        else:
            sheet['D{}'.format(max_index + 2)].value = 'FAIL'
        if source_row == target_row:
            sheet['D{}'.format(max_index + 3)].value = 'PASS'
        else:
            sheet['D{}'.format(max_index + 3)].value = 'FAIL'
        sheet['B{}'.format(max_index + 4)].value = datetime.datetime.now()

        writer.save()

        return True

    def check_null(self, column_name, testcase_id):
        df = self.target_df
        nan_rows = pd.DataFrame()

        for column in column_name:
            nan_rows = nan_rows.append(df[self.target_df[column].isnull()])

        book = load_workbook(self.pathname)
        writer = pd.ExcelWriter(self.pathname)
        writer.book = book
        if testcase_id in book.sheetnames:
            name = book.get_sheet_by_name(testcase_id)
            book.remove_sheet(name)
        output_reduce = nan_rows.head(n=100)
        output_reduce.to_excel(writer, sheet_name=str(testcase_id), index=False, startrow=3)
        sheet = book.get_sheet_by_name(str(testcase_id))
        max_index = sheet.max_row
        sheet['A2'].value = "NULL CHECK --- Target DB"
        # Make the text of the cell bold and italic
        cell = sheet['A{}'.format(max_index + 1)]
        cell.font = cell.font.copy(bold=True)
        sheet['A{}'.format(max_index + 1)].value = 'Total Failure Count'
        sheet['B{}'.format(max_index + 1)].value = len(nan_rows)
        sheet['A{}'.format(max_index + 2)].value = 'Execution TimeStamp'
        sheet['B{}'.format(max_index + 2)].value = datetime.datetime.now()
        writer.save()
        if len(output_reduce) == 0:
            return True
        else:
            print len(output_reduce)
            return False

    def onebyone_check(self, testcase_id, targetColumn):
        try:
            book = load_workbook(self.pathname)
            writer = pd.ExcelWriter(self.pathname)
            writer.book = book
            if testcase_id in book.sheetnames:
                name = book.get_sheet_by_name(testcase_id)
                book.remove_sheet(name)

            if self.targetPrimaryKey != '':
                indexed_df = self.target_df.set_index(self.targetPrimaryKey)
            else:
                pass
            if self.sourcePrimaryKey != '':
                indexed_df2 = self.source_df.set_index(self.sourcePrimaryKey)
            else:
                pass

            for column in indexed_df:
                if column in targetColumn:
                    # moves the control back to the top of the loop
                    continue
                else:
                    indexed_df = indexed_df.drop(column, axis=1)
                    # print 'dropped {}'.format(column)
            for column in indexed_df2:
                if column in targetColumn:
                    # moves the control back to the top of the loop
                    continue
                else:
                    indexed_df2 = indexed_df2.drop(column, axis=1)
                    # print 'dropped {}'.format(column)

            # print indexed_df,indexed_df2


            diff_panel = pd.Panel(dict(df1=indexed_df2, df2=indexed_df))

            # Applying the diff function
            diff_output = diff_panel.apply(self.report_diff, axis=0)

            # Flag all the changes
            diff_output['has_change'] = diff_output.apply(self.has_change, axis=1)

            output_reduce = diff_output[(diff_output.has_change == 'Y')]

            output_reduce.head(n=100).to_excel(writer, sheet_name=str(testcase_id), startrow=3)
            sheet = book.get_sheet_by_name(str(testcase_id))
            max_index = sheet.max_row
            sheet['A1'].value = 'Source ----> Target'
            sheet['C2'].value = "DATA COMPARISON"
            sheet['A{}'.format(max_index + 1)].value = 'Total Failure Count:'
            sheet['B{}'.format(max_index + 1)].value = len(output_reduce)
            max_index = sheet.max_row
            sheet['A{}'.format(max_index + 1)].value = 'Execution TimeStamp'
            sheet['B{}'.format(max_index + 1)].value = datetime.datetime.now()

            # red_text = Font(color="9C0006")
            # red_fill = PatternFill(bgColor="FFC7CE")
            # dxf = DifferentialStyle(font=red_text, fill=red_fill)
            # rule = Rule(type="containsText", operator="containsText", text="highlight", dxf=dxf)
            # rule.formula = ['NOT(ISERROR(SEARCH("--->")))']
            # writer.conditional_formatting.add('A1:F40', rule)

            # writer.save()
            if len(output_reduce) == 0:
                diff_output.head(n=100).to_excel(writer, sheet_name=str(testcase_id), startrow=3)
                max_index = sheet.max_row
                sheet['A{}'.format(max_index + 1)].value = 'Execution TimeStamp'
                sheet['B{}'.format(max_index + 1)].value = datetime.datetime.now()
                writer.save()
                return True
            else:
                print len(output_reduce)
                writer.save()
                return False
        except Exception as e:
            print e
            return False

    def report_diff(self, x):
        # diff function to show the changes in each field
        return x[0] if x[0] == x[1] else '{} ---> {}'.format(*x)

    def has_change(self, row):
        # tell which rows have changes
        if "--->" in row.to_string():
            return "Y"
        else:
            return "N"

    def update_result(self, testcase_id, testdata='', override=''):
        try:
            # files = [f for f in listdir('./results') if re.match(r'{}+.*\.xlsx'.format(testcase_id), f)]
            # for file in files:
            #     check_df = pd.read_excel("./results/{}".format(file))
            #     check_index_len += len(check_df.index)
            check_index_len = 0
            check_df = pd.read_excel(self.pathname, sheet_name=str(testcase_id))
            check_index_len += len(check_df.index)
            work_book = load_workbook(self.pathname)
            sheet = work_book.get_sheet_by_name(self.selected_sheet)
            len_row = sheet.max_row
            sheet2 = work_book.get_sheet_by_name(str(testcase_id))

            if sheet2['D4'].value == 'PASS' or override == 'PASS':
                override = 'PASS'
            else:
                override = 'FAIL'
            print check_index_len
            if check_index_len < 3 or override == 'PASS':
                for cellObj in sheet['A1':'B{}'.format(len_row)]:
                    for cell in cellObj:
                        #print(cell.coordinate, cell.value)
                        if cell.value == testcase_id:
                            cell_index = map(int, re.findall('\d+', cell.coordinate))
                            sheet['B{}'.format(cell_index[0])].value = 'PASS'
                            #work_book.create_sheet(str(testcase_id))
                            link = "{}#{}".format(self.pathname,str(testcase_id))
                            sheet['B{}'.format(cell_index[0])].hyperlink = link
                            sheet.cell(row=1, column=1).hyperlink = link
                            if testdata != '':
                                sheet_data = work_book.get_sheet_by_name(str(testcase_id))

                            break
                work_book.save(self.pathname)
                return True
            elif check_index_len < 0 or override == 'FAIL':
                for cellObj in sheet['A1':'B{}'.format(len_row)]:
                    for cell in cellObj:
                        #print(cell.coordinate, cell.value)
                        if cell.value == testcase_id:
                            cell_index = map(int, re.findall('\d+', cell.coordinate))
                            sheet['B{}'.format(cell_index[0])].value = 'FAIL'
                            #work_book.create_sheet(str(testcase_id))
                            link = "{}#{}".format(self.pathname, str(testcase_id))
                            sheet['B{}'.format(cell_index[0])].hyperlink = link
                            sheet.cell(row=1, column=1).hyperlink = link
                            if testdata != '':
                                sheet_data = work_book.get_sheet_by_name(str(testcase_id))
                            break
                    work_book.save(self.pathname)
                return False
        except Exception as e:
            print e
            return False

    def create_results_sheet(self, sheetnames):
        work_book = load_workbook(self.pathname)
        # print sheetnames,type(sheetnames)

        for sheet in sheetnames:
            if str(sheet) in work_book.sheetnames:
                # work_book.get_sheet_by_name(str(sheet)).title = str(sheet) + '_old'
                name = work_book.get_sheet_by_name(str(sheet))
                work_book.remove_sheet(name)
            work_book.create_sheet(str(sheet))
        work_book.save(self.pathname)
        return True

    def get_testdata(self, test_selected):
        df_data = pd.DataFrame()
        dict_data = {}
        tc_index = ''
        pd.set_option('max_colwidth',1024)
        for tc_id in test_selected:
            df_data = self.df_testcase.loc[[tc_id]]
            str_tcid = df_data.to_string(columns=['Test Case ID'], index=False, header=False)
            str_data = df_data.to_string(columns=['Title'], index=False, header=False)
            str_test_class = df_data.to_string(columns=['Test Class'], index=False, header=False)
            str_query = df_data.to_string(columns=['Test queries'], index=False, header=False)
            str_data = str_data.encode('utf8')
            str_tcid = str_tcid.encode('utf8')
            str_test_class = str_test_class.encode('utf8')
            str_query = str_query.encode('utf8')

            str_data = re.split('@|\n|\\n|[|]',str_data)
            str_query = re.split('@|\\n|:', str_query)
            str_data.insert(0, str_tcid)
            str_data.insert(1, str_test_class)
            for itr in str_data:
                itr = itr.encode('unicode_escape')
                itr = re.sub('[\s+]', '', itr)
                itr = re.split(':|\n|\s|\\\\', itr)
                if "TC_" in itr[0]:
                    test_class = str_data[1]
                    dict_data[itr[0]+'_'+test_class] = {}
                    tc_index = itr[0]+'_'+test_class
                    dict_data[tc_index]['testClass'] = test_class
                    if 'querySource' in str_query:
                        squery_index = str_query.index('querySource')
                        sourceQuery = str_query[squery_index + 1]
                        sourceQuery = sourceQuery.replace('\'', '')
                        dict_data[tc_index]['querySource'] = sourceQuery.replace('\\n', '')
                    if 'queryTarget' in str_query:
                        tquery_index = str_query.index('queryTarget')
                        targetQuery = str_query[tquery_index + 1]
                        dict_data[tc_index]['queryTarget'] = targetQuery.replace('\\n', '')

                elif "sourcedbType" in itr:
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n',"")
                    itr = itr.replace(':',"")
                    itr = itr.replace('\\',"")
                    dict_data[tc_index]['sourcedbType'] = itr

                elif "sourceServer" in itr:
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n', "")
                    itr = itr.replace(':', "")
                    itr = itr.replace('\\', "")
                    dict_data[tc_index]['sourceServer'] = itr

                elif "sourcedb" in itr:
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n', "")
                    itr = itr.replace(':', "")
                    itr = itr.replace('\\', "")
                    dict_data[tc_index]['sourcedb'] = itr

                elif "sourceTable" in itr:
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n', "")
                    itr = itr.replace(':', "")
                    itr = itr.replace('\\', "")
                    dict_data[tc_index]['sourceTable'] = itr

                elif "sourceQuery" in itr :
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n', "")
                    itr = itr.replace(':', "")
                    itr = itr.replace('\\', "")
                    dict_data[tc_index]['sourceQuery'] = itr

                elif "sourceColumn" in itr:
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n', "")
                    itr = itr.replace(':', "")
                    itr = itr.replace('\\', "")
                    dict_data[tc_index]['sourceColumn'] = itr

                elif "targetdbType" in itr:
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n', "")
                    itr = itr.replace(':', "")
                    itr = itr.replace('\\', "")
                    dict_data[tc_index]['targetdbType'] = itr

                elif "targetServer" in itr:
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n', "")
                    itr = itr.replace(':', "")
                    itr = itr.replace('\\', "")
                    dict_data[tc_index]['targetServer'] = itr

                elif "targetdb" in itr :
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n', "")
                    itr = itr.replace(':', "")
                    itr = itr.replace('\\', "")
                    dict_data[tc_index]['targetdb'] = itr

                elif "targetTable" in itr:
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n', "")
                    itr = itr.replace(':', "")
                    itr = itr.replace('\\', "")
                    dict_data[tc_index]['targetTable'] = itr

                elif "targetQuery" in itr:
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n', "")
                    itr = itr.replace(':', "")
                    itr = itr.replace('\\', "")
                    dict_data[tc_index]['targetQuery'] = itr

                elif "targetColumn" in itr:
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n', "")
                    itr = itr.replace(':', "")
                    itr = itr.replace('\\', "")
                    dict_data[tc_index]['targetColumn'] = itr

                elif "sourcePrimaryKey" in itr:
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n', "")
                    itr = itr.replace(':', "")
                    itr = itr.replace('\\', "")
                    dict_data[tc_index]['sourcePrimaryKey'] = itr

                elif "targetPrimaryKey" in itr:
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n', "")
                    itr = itr.replace(':', "")
                    itr = itr.replace('\\', "")
                    dict_data[tc_index]['targetPrimaryKey'] = itr

                elif "excludeColumns" in itr:
                    itr = itr[1].replace('\\n', "")
                    itr = itr.replace('\n', "")
                    itr = itr.replace(':', "")
                    itr = itr.replace('\\', "")
                    dict_data[tc_index]['excludeColumns'] = itr
        return dict_data
        # pd.reset_option('max_colwidth')

    def read_creds(self):
        try:
            if path.exists("creds") != '':
                dict_creds = {}
                with open("creds") as f:
                    for line in f:

                        if line != '\n' and line != '':
                            (key, val) = line.split("=")

                            if 'serverName' in key:
                                dict_key = val.replace('\n', '')
                                dict_creds[dict_key] = {}
                                dict_creds[dict_key][key] = val.replace('\n', '')

                            elif line != '':
                                dict_creds[dict_key][key] = val.replace('\n', '')
                print dict_creds
            else:
                print 'File Not Available in Path , Retry Checking Creds File'

            return dict_creds
        except IOError:
            wx.LogError("Cannot open file '%s'." % file)

# -----------------------UNUSED DEF-------------------------------------

    def check_metadata(self,testcase_id):

        # print self.source_meta
        # print self.target_meta

        self.dict_compare(self.source_meta, self.target_meta, testcase_id)

        print "METADATA CHECK DONE !!!"

    def index_check(self):
        #unused method
        df = pd.concat([self.source_df, self.target_df])
        df = df.reset_index(drop=True)
        #group by
        df_gpby = df.groupby(list(df.index))
        # get index of unique records
        idx = [x[0] for x in df_gpby.groups.values() if len(x) == 1]
        #filter
        print 'Index Change Check Before Comparison:{}'.format(df.reindex(idx))
        return df.reindex(idx)

    def df_comparison(self,testcase_id):
        try:

            book = load_workbook(self.pathname)
            writer = pd.ExcelWriter(self.pathname)
            writer.book = book
            if testcase_id in book.sheetnames:
                name = book.get_sheet_by_name(testcase_id)
                book.remove_sheet(name)
                # print 'Duplicate Sheet Deleted'

            df = self.source_df
            df2 = self.target_df
            primaryKey = self.targetPrimaryKey
            index_missing = df[~self.source_df[primaryKey].isin(self.target_df[primaryKey])]
            index_missing2 = df2[~self.target_df[primaryKey].isin(self.source_df[primaryKey])]

            if len(index_missing) == 0 and len(index_missing2) == 0 :
                # Create a panel of the two dataframes
                diff_panel = pd.Panel(dict(df1=self.source_df,df2=self.target_df))

                #Applying the diff function
                diff_output = diff_panel.apply(self.report_diff, axis=0)

                # Flag all the changes
                diff_output['has_change'] = diff_output.apply(self.has_change, axis=1)

                # writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
                #Save the changes to excel but only include the columns changed

                diff_output[(diff_output.has_change == 'Y')].to_excel(writer,sheet_name=str(testcase_id),index=False,columns=list(diff_output))
                sheet = book.get_sheet_by_name(str(testcase_id))
                max_index = sheet.max_row
                # Make the text of the cell bold and italic
                cell = sheet['A{}'.format(max_index + 1)]
                cell.font = cell.font.copy(bold=True)
                sheet['A{}'.format(max_index + 1)].value = 'Execution TimeStamp'
                sheet['B{}'.format(max_index+1)].value = datetime.datetime.now()
                writer.save()

                print "ONE-TO-ONE COMPARISON DONE FOR : {}".format(testcase_id)

                return True
            else:
                print "ONE-TO-ONE COMPARISON DONE FOR : {}".format(testcase_id)
                # sheet = book.get_sheet_by_name(str(testcase_id))
                # max_index = sheet.max_row
                # cell = sheet['A{}'.format(max_index + 1)]
                # cell.font = cell.font.copy(bold=True)
                # sheet['A{}'.format(max_index + 1)].value = 'ROWS NOT EXISTS IN TARGET TABLE'
                # max_index = sheet.max_row
                index_missing.to_excel(writer, sheet_name=str(testcase_id), index=False,startrow=3)
                sheet = book.get_sheet_by_name(str(testcase_id))
                cell = sheet['A2']
                cell.font = cell.font.copy(bold=True)
                sheet['A2'].value = 'ROWS NOT EXISTS IN TARGET TABLE'
                max_index = sheet.max_row
                cell = sheet['A{}'.format(max_index + 1)]
                cell.font = cell.font.copy(bold=True)
                sheet['A{}'.format(max_index + 1)].value = 'ROWS NOT EXISTS IN SOURCE TABLE'
                max_index = sheet.max_row
                index_missing2.to_excel(writer, sheet_name=str(testcase_id), index=False,startrow=max_index+1)
                max_index = sheet.max_row
                sheet['A{}'.format(max_index + 1)].value = 'Execution TimeStamp'
                sheet['B{}'.format(max_index + 1)].value = datetime.datetime.now()
                writer.save()
                return False

        except Exception as e:
            print e
            return False

    def dict_compare(self, d1, d2, testcase_id):
        ddl_dict = {}    # Source metadata
        ddl2_dict = {}   # Target metadata
        max_index = 1
        book = load_workbook(self.pathname)
        writer = pd.ExcelWriter(self.pathname)
        sheet = book.get_sheet_by_name(str(testcase_id))
        writer.book = book
        sheet['A1'].value = 'DDL Check'
        sheet['A2'].value = 'Column Name'
        sheet['B2'].value = 'Data Type'
        sheet['C2'].value = 'Nullable'
        sheet['D2'].value = 'Column Missing '
        for i in d1:
            col = i['name']
            ddl_dict[col] = [i]

        for i2 in d2:
            col2 = i2['name']
            ddl2_dict[col2] = [i2]

        for key in ddl_dict.keys():

            if key in ddl2_dict.keys():
                if str(ddl_dict[key][0]['type']) == str(ddl2_dict[key][0]['type']):
                    if str(ddl_dict[key][0]['nullable']) == str(ddl2_dict[key][0]['nullable']):
                        # print "Name Type Nullable Comparison True : {}".format(ddl_dict[key])
                        continue
                    else:
                        max_index = sheet.max_row
                        cell2 = sheet['A{}'.format(max_index + 1)]
                        cell2.font = cell2.font.copy(bold=True)
                        sheet['A{}'.format(max_index + 1)].value = key
                        sheet['B{}'.format(max_index + 1)].value = str(ddl_dict[key][0]['type'])
                        sheet['C{}'.format(max_index + 1)].value = str(ddl_dict[key][0]['nullable'])
                        sheet['D{}'.format(max_index + 1)].value = 'IN Target'
                        writer.save()
                        print "Column Nullable Mismatch : {} \n with \n {}".format(ddl2_dict[key], ddl_dict[key])
                else:
                    max_index = sheet.max_row
                    cell2 = sheet['A{}'.format(max_index + 1)]
                    cell2.font = cell2.font.copy(bold=True)
                    sheet['A{}'.format(max_index + 1)].value = key
                    sheet['B{}'.format(max_index + 1)].value = str(ddl_dict[key][0]['type'])
                    sheet['C{}'.format(max_index + 1)].value = str(ddl_dict[key][0]['nullable'])
                    sheet['D{}'.format(max_index + 1)].value = 'IN Target'
                    writer.save()
                    print "Column Type Mismatch : {} \n with \n {}".format(ddl2_dict[key], ddl_dict[key])

            else:
                max_index = sheet.max_row
                cell2 = sheet['A{}'.format(max_index + 1)]
                cell2.font = cell2.font.copy(bold=True)
                sheet['A{}'.format(max_index + 1)].value = key
                sheet['B{}'.format(max_index + 1)].value = str(ddl_dict[key][0]['type'])
                sheet['C{}'.format(max_index + 1)].value = str(ddl_dict[key][0]['nullable'])
                sheet['D{}'.format(max_index + 1)].value = 'IN Target'
                writer.save()
                print "Column Not Found in Target Database :{}".format(ddl_dict[key])

        for key in ddl2_dict.keys():

            if key in ddl_dict.keys():
                if str(ddl_dict[key][0]['type']) == str(ddl2_dict[key][0]['type']):
                    if str(ddl_dict[key][0]['nullable']) == str(ddl2_dict[key][0]['nullable']):
                        # print "Name Type Nullable Comparison True : {}".format(ddl_dict[key])
                        continue
                    else:
                        max_index = sheet.max_row
                        cell2 = sheet['A{}'.format(max_index + 1)]
                        cell2.font = cell2.font.copy(bold=True)
                        sheet['A{}'.format(max_index + 1)].value = key
                        sheet['B{}'.format(max_index + 1)].value = str(ddl2_dict[key][0]['type'])
                        sheet['C{}'.format(max_index + 1)].value = str(ddl2_dict[key][0]['nullable'])
                        sheet['D{}'.format(max_index + 1)].value = 'IN Source'
                        writer.save()
                        print "Column Nullable Mismatch : {} \n with \n {}".format(ddl2_dict[key], ddl_dict[key])
                else:
                    max_index = sheet.max_row
                    cell2 = sheet['A{}'.format(max_index + 1)]
                    cell2.font = cell2.font.copy(bold=True)
                    sheet['A{}'.format(max_index + 1)].value = key
                    sheet['B{}'.format(max_index + 1)].value = str(ddl2_dict[key][0]['type'])
                    sheet['C{}'.format(max_index + 1)].value = str(ddl2_dict[key][0]['nullable'])
                    sheet['D{}'.format(max_index + 1)].value = 'IN Source'

                    writer.save()
                    print "Column Type Mismatch : {} \n with \n {}".format(ddl2_dict[key], ddl_dict[key])

            else:
                max_index = sheet.max_row
                cell2 = sheet['A{}'.format(max_index + 1)]
                cell2.font = cell2.font.copy(bold=True)
                sheet['A{}'.format(max_index + 1)].value = key
                sheet['B{}'.format(max_index + 1)].value = str(ddl2_dict[key][0]['type'])
                sheet['C{}'.format(max_index + 1)].value = str(ddl2_dict[key][0]['nullable'])
                sheet['D{}'.format(max_index + 1)].value = 'IN Source'
                writer.save()
                print "Column Not Found in Source Database :{}".format(ddl2_dict[key])

        max_index = sheet.max_row
        cell2 = sheet['A{}'.format(max_index + 1)]
        cell2.font = cell2.font.copy(bold=True)
        sheet['A{}'.format(max_index + 1)].value = 'Execution TimeStamp'
        sheet['B{}'.format(max_index + 1)].value = datetime.datetime.now()

        writer.save()

    def OnSelectAll(self, event):
        num = self.list.GetItemCount()
        for i in range(num):
            self.list.CheckItem(i)

    def OnDeselectAll(self, event):
        num = self.list.GetItemCount()
        for i in range(num):
            self.list.CheckItem(i, False)



