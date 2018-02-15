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
# import pandasql as pdsql
import wx
from os import path
import re
from openpyxl import load_workbook
from openpyxl.styles import Font
import datetime


class App():

    def __init__(self):
        # self.pysql = lambda q: pdsql.sqldf(q, globals())

        self.pathname = self.open_filedialog()
        self.df_testcase,self.selected_sheet = self.read_testcase(self.pathname)
        test_selected = self.select_testcases(self.df_testcase)
        self.dict_creds = self.read_creds()
        self.test_data = self.get_testdata(test_selected)
        for key,value in self.test_data.iteritems():
            self.testcase_id = key
            self.source_df,self.target_df= self.create_datasource(self.testcase_id)
            if self.testClass == 'DataValidation' or self.testClass == 'Data Validation':
                if self.df_comparison(self.testcase_id):
                    if self.update_result(self.testcase_id):
                        print "{} Executed and Results as PASS Updated ".format(self.testcase_id)
                    else :
                        print "{} Executed and Results as FAIL Updated ".format(self.testcase_id)
                else:
                    self.update_result(self.testcase_id)
                    print "{} Executed and Results as FAIL Updated ".format(self.testcase_id)

            elif self.testClass == 'CountCheck':
                if self.check_count(self.testcase_id):
                    if self.update_result(self.testcase_id):
                        print "{} Executed and Results as PASS Updated ".format(self.testcase_id)
                    else :
                        print "{} Executed and Results as FAIL Updated ".format(self.testcase_id)

            elif self.testClass == 'DuplicateCheck':
                self.targetColumn = self.select_columns(self.excludeColumns)
                # column names as a list to verify duplicates on each columns
                if self.targetColumn != '':
                    if self.check_duplicates(self.targetColumn,self.testcase_id, 'target'):
                        if self.update_result(self.testcase_id):
                            print "{} Executed and Results as PASS Updated ".format(self.testcase_id)
                        else :
                            print "{} Executed and Results as FAIL Updated ".format(self.testcase_id)
                else:
                    print "Make Sure you have selected Column Name for Checking Duplicates and Retry"
            elif self.testClass == 'DDLCheck':
                print "Not Implemented DDLCheck"

            elif self.testClass == 'LoadStrategy':
                print "Not Implemented LoadStrategy"

            else:
                print "Please Add a Test Class for framework to know how to Execute Case"

    # def insert_sql(self, sql_command):
    #     # Helper for Query in a dataframe and return the transit dataframe --- business logic
    #     df_source = self.source_df
    #     print df_source
    #     df_transition = self.pysql(sql_command)
    #     return df_transition
    #     pass

    def check_metadata(self):
        df_source_metadata = self.df_source.info(memory_usage=False)
        df_target_metadata = self.df_target.info(memory_usage=False)

        print df_target_metadata, df_source_metadata

        df_check = pd.Panel(dict(df_source_metadata=df_source_metadata,df_target_metadata=df_target_metadata))
        df_meta_changes = df_check.apply(self.report_diff, axis=0)
        df_meta_changes['has_change'] = df_meta_changes.apply(self.has_change, axis=1)
        df_meta_changes[(df_meta_changes.has_change == 'Y')].to_excel('metadata_check.xlsx', index=False, columns=list(df_meta_changes))
        print "METADATA CHECK DONE !!!"

    def check_duplicates(self,column_name,testcase_id,source_name):
        column_name = [x.encode('utf-8') for x in column_name ]
        print "Duplicate Check on Columns : {}".format(column_name)
        # column_name = re.sub('\'|\[|\]|\"','',column_name)
        # column_name = re.split(',',column_name)
        max_index = 1
        if source_name == 'target':
            book = load_workbook(self.pathname)
            sheet = book.get_sheet_by_name(str(testcase_id))
            for column in column_name:
                print '{}_Duplicates Check on {}'.format(testcase_id,column)
                df_dupes = self.target_df[self.target_df.duplicated(column,keep=False)]
                if column != '':
                    writer = pd.ExcelWriter(self.pathname)
                    writer.book = book
                    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
                    # Save the changes to excel but only include the columns changed
                    cell = sheet['A{}'.format(max_index)]
                    cell.font = cell.font.copy(bold=True)
                    sheet['A{}'.format(max_index + 1)].value = column
                    df_dupes.to_excel(writer, sheet_name=str(testcase_id),startrow=max_index+1)
                    max_index = sheet.max_row

            # Make the text of the cell bold and italic
            cell2 = sheet['A{}'.format(max_index + 1)]
            cell2.font = cell2.font.copy(bold=True)
            sheet['A{}'.format(max_index + 1)].value = 'Execution TimeStamp'
            sheet['B{}'.format(max_index + 1)].value = datetime.datetime.now()
            writer.save()


            return True
        else:
            print 'To be Checked in Target DB , Enter the Source Name Correctly !!'
            return False

    def check_count(self,testcase_id):

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

    def report_diff(self, x):
        # diff function to show the changes in each field
        return x[0] if x[0] == x[1] else '{} ---> {}'.format(*x)

    def has_change(self, row):
        # tell which rows have changes
        if "--->" in row.to_string():
            return "Y"
        else:
            return "N"

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
                print 'Duplicate Sheet Deleted'

            df = self.source_df
            df2 = self.target_df
            primaryKey = self.targetPrimaryKey
            index_missing = df[~self.source_df[primaryKey].isin(self.target_df[primaryKey])]
            index_missing2 = df2[~self.target_df[primaryKey].isin(self.source_df[primaryKey])]

            if len(index_missing) == 0 :
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
                    wx.LogError("Cannot open file '%s'." %file)
        except Exception as e:
            app.Destroy()
            print e

    def select_columns(self,excludeColumns):
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
                                       "Pick The Columns For Duplicate Check",
                                       "Aciom", column_list)

            if (dlg.ShowModal() == wx.ID_OK):
                selections = dlg.GetSelections()
                strings = [column_list[x] for x in selections]
                print "You chose:" + str(strings)
                return strings

    def read_creds(self):
        try:
            if path.exists("creds") != '':
                dict_creds = {}
                with open("creds") as f:
                    for line in f:

                        if line != '\n' and line != '':
                            (key, val) = line.split("=")

                            if 'serverName' in key:
                                dict_key = val.replace('\n','')
                                dict_creds[dict_key] = {}
                                dict_creds[dict_key][key] = val.replace('\n','')

                            elif line != '':
                                dict_creds[dict_key][key] = val.replace('\n','')
            else:
                print 'File Not Available in Path , Retry Checking Creds File'

            return dict_creds
        except IOError:
            wx.LogError("Cannot open file '%s'." % file)

    def read_testcase(self,pathname):
        try:
            pd.set_option('max_colwidth', 1024)
            if pathname != '':
                df_excel_sheet = pd.ExcelFile(pathname)
                sheet_list = df_excel_sheet.sheet_names
                selected_sheet = self.select_sheet(sheet_list)
                selected_sheet = selected_sheet[0].encode('utf8')
                df_excel = pd.read_excel(pathname,sheet_name=selected_sheet)
            else:
                print 'No Pathname Available , Retry Selecting the File'
            #print df_excel
            return df_excel,selected_sheet
        except IOError:
            wx.LogError("Cannot open file '%s'." % file)

    def select_testcases(self,df_testcases):
        app = wx.App()

        frame = wx.Frame(None, -1, 'Select Tests')
        frame.SetSize(0,0,200,50)

        test_list = df_testcases['Test Case ID']
        dlg = wx.MultiChoiceDialog(frame,
                                   "Pick The TestCases To Be Executed",
                                   "TestPandas", test_list)

        if (dlg.ShowModal() == wx.ID_OK):
            selections = dlg.GetSelections()
            strings = [test_list[x] for x in selections]
            print "You chose:" + str(strings)

            self.create_results_sheet(strings)
            return selections

    def select_sheet(self,sheet_list):
        app = wx.App()

        frame = wx.Frame(None, -1, 'Select Sheet For Test Cases')
        frame.SetSize(0,0,200,50)

        dlg = wx.MultiChoiceDialog(frame,
                                   "Pick The SheetName To View TestCases",
                                   "TestPandas", sheet_list)

        if (dlg.ShowModal() == wx.ID_OK):
            selections = dlg.GetSelections()
            strings = [sheet_list[x] for x in selections]
            print "You choose SheetName:" + str(strings)
            return strings

    def create_datasource(self,testcase_id):
        try:
            if self.test_data[testcase_id]:
                self.targetQuery = ''
                self.sourceQuery = ''
                self.targetColumn = ''
                self.sourceColumn = ''
                self.sourcePrimaryKey = self.test_data[testcase_id]['sourcePrimaryKey']
                self.sourcedbType = self.test_data[testcase_id]['sourcedbType']
                self.sourcedb = self.test_data[testcase_id]['sourcedb']
                self.sourceServer = self.test_data[testcase_id]['sourceServer']
                self.sourceTable = self.test_data[testcase_id]['sourceTable']
                self.targetPrimaryKey = self.test_data[testcase_id]['targetPrimaryKey']
                self.targetdbType = self.test_data[testcase_id]['targetdbType']
                self.targetdb = self.test_data[testcase_id]['targetdb']
                self.targetServer = self.test_data[testcase_id]['targetServer']
                self.targetTable = self.test_data[testcase_id]['targetTable']
                self.testClass = self.test_data[testcase_id]['testClass']
                if 'sourceColumn' in self.test_data[testcase_id]:
                    self.sourceColumn = self.test_data[testcase_id]['sourceColumn']
                if 'targetColumn' in self.test_data[testcase_id]:
                    self.targetColumn = self.test_data[testcase_id]['targetColumn']

                if 'targetQuery' in self.test_data[testcase_id]:
                    self.targetQuery = self.test_data[testcase_id]['targetQuery']
                if 'sourceQuery' in self.test_data[testcase_id]:
                    self.sourceQuery = self.test_data[testcase_id]['sourceQuery']
                if 'excludeColumns' in self.test_data[testcase_id]:
                    self.excludeColumns = self.test_data[testcase_id]['excludeColumns']
            else:
                print "TestCase ID not matched/ Not Found"

            if self.sourcedb != '' and self.sourceServer != '' and self.sourcedbType != '':
                source_engine = self.create_engine(self.dict_creds,self.sourceServer,self.sourcedbType,self.sourcedb)
                source_df = self.create_dataframe(source_engine,self.sourcePrimaryKey,self.targetTable,self.sourceQuery)

            if self.targetdb != '' and self.targetServer != '' and self.targetdbType != '':
                target_engine = self.create_engine(self.dict_creds,self.targetServer,self.targetdbType,self.targetdb)
                target_df = self.create_dataframe(target_engine,self.sourcePrimaryKey,self.targetTable,self.targetQuery)

            return source_df,target_df
        except Exception as e:
            print e

    def get_testdata(self,test_selected):
        df_data = pd.DataFrame()
        dict_data ={}
        tc_index = ''
        pd.set_option('max_colwidth',1024)
        for tc_id in test_selected:
            df_data = self.df_testcase.loc[[tc_id]]
            str_tcid = df_data.to_string(columns=['Test Case ID'], index=False,header=False)
            str_data = df_data.to_string(columns=['Title'], index=False,header=False)
            str_test_class = df_data.to_string(columns=['Test Class'], index=False,header=False)
            str_query = df_data.to_string(columns=['Test queries'], index=False,header=False)
            str_data = str_data.encode('utf8')
            str_tcid = str_tcid.encode('utf8')
            str_test_class = str_test_class.encode('utf8')
            str_query = str_query.encode('utf8')

            str_data = re.split('@|\n|\\n|[|]',str_data)
            str_query = re.split('@|\\n|:',str_query)
            str_data.insert(0,str_tcid)
            str_data.insert(1,str_test_class)
            for itr in str_data:
                itr = itr.encode('unicode_escape')
                itr = re.sub('[\s+]', '', itr)
                itr = re.split(':|\n|\s|\\\\',itr)
                if "TC_" in itr[0]:
                    test_class = str_data[1]
                    dict_data[itr[0]] = {}
                    tc_index = itr[0]
                    dict_data[tc_index]['testClass'] = test_class
                    if 'sourceQuery' in str_query:
                        squery_index = str_query.index('sourceQuery')
                        sourceQuery = str_query[squery_index + 1]
                        dict_data[tc_index]['sourceQuery'] = sourceQuery.replace('\\n','')
                    if 'targetQuery' in str_query:
                        tquery_index = str_query.index('targetQuery')
                        targetQuery = str_query[tquery_index + 1]
                        dict_data[tc_index]['targetQuery'] = targetQuery.replace('\\n','')

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

    def create_engine(self,creds,serverName,dbType,dbName):
        try:
        # dialect+driver://username:password@host:port/database
        # http://docs.sqlalchemy.org/en/latest/core/engines.html
            userName = creds[serverName]['userName']
            databaseType = creds[serverName]['databaseType']
            port = creds[serverName]['port']
            passWord = creds[serverName]['passWord']
            if dbType == 'postgres':
                db_engine = create_engine('postgresql://%s:%s@%s:%s/%s'%(userName,passWord,serverName,port,dbName))
                return db_engine
            elif dbType == 'oracle':
                #oracle+cx_oracle://user:pass@host:port/dbname[?key=value&key=value...]
                db_engine = create_engine('oracle+cx_oracle://%s:%s@%s:%s/%s'%(userName,passWord,serverName,port,dbName))

        except Exception as e:
            print e

    def create_dataframe(self,engine,primaryKey,targetTable,logic=''):
     # Query Database Table and return a DataFrame
        if logic != '':
            return_df = pd.read_sql("%s" %logic,con=engine)
            return return_df

        else:
            return_df = pd.read_sql("SELECT * FROM %s ORDER BY %s ASC;" %(targetTable,primaryKey), con=engine)
            return return_df

    def update_result(self,testcase_id,testdata = ''):
        try:
            # files = [f for f in listdir('./results') if re.match(r'{}+.*\.xlsx'.format(testcase_id), f)]
            check_index_len = 0
            override = ''
            # for file in files:
            #     check_df = pd.read_excel("./results/{}".format(file))
            #     check_index_len += len(check_df.index)

            check_df = pd.read_excel(self.pathname,sheet_name=str(testcase_id))
            check_index_len += len(check_df.index)
            work_book = load_workbook(self.pathname)
            sheet = work_book.get_sheet_by_name(self.selected_sheet)
            len_row = sheet.max_row
            sheet2 = work_book.get_sheet_by_name(str(testcase_id))
            if sheet2['D4'].value == 'PASS':
                override = 'PASS'
            else:
                override = 'FAIL'

            if check_index_len == 0 or override == 'PASS':
                for cellObj in sheet['A1':'B{}'.format(len_row)]:
                    for cell in cellObj:
                        #print(cell.coordinate, cell.value)
                        if cell.value == testcase_id:
                            cell_index = map(int, re.findall('\d+', cell.coordinate))
                            sheet['B{}'.format(cell_index[0])].value = 'PASS'
                            #work_book.create_sheet(str(testcase_id))
                            link = "{}#{}".format(self.pathname,str(testcase_id))
                            sheet['B{}'.format(cell_index[0])].hyperlink = (link)
                            # ws.cell(row=1, column=1).hyperlink = (link)
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
                            if testdata != '':
                                sheet_data = work_book.get_sheet_by_name(str(testcase_id))
                            break
                    work_book.save(self.pathname)
                return False
        except Exception as e:
            print e
            return False

    def create_results_sheet(self,sheetnames):
        work_book = load_workbook(self.pathname)
        print sheetnames,type(sheetnames)

        for sheet in sheetnames:
            if str(sheet) in work_book.sheetnames:
                work_book.get_sheet_by_name(str(sheet)).title = str(sheet) + '_old'
            work_book.create_sheet(str(sheet))
        work_book.save(self.pathname)
        return True



