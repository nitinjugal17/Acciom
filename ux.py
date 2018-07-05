
import wx
import pandas as pd
import utilsexcel as ue
import inspect as ins
import re
from testux import SelectAll



def open_filedialog():
    try:
        app = wx.App()

        frame = wx.Frame(None, -1, 'Select XLSX File')
        frame.SetSize(0, 0, 200, 50)
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
        print ins.stack()[0][3]


def select_testcases(df_testcases, pathname):
    try:
        app = wx.App()
        df = pd.DataFrame()
        #frame = wx.Frame(None, -1, 'Select Tests')
        #frame.SetSize(0, 0, 200, 50)

        df['Test Case'] = df_testcases['Test Case ID'].astype(str) + '_' + df_testcases['Test Class']
        test_list = df['Test Case']

        dlg = SelectAll(None, "Pick The TestCases To Be Executed",
                                   "Acciom", choices=test_list)

        if dlg.ShowModal() == wx.ID_OK:
            selections = dlg.GetSelections()
            strings = [test_list[x] for x in selections]
            wx.MessageBox(str(strings) + ' were chosen')
            print "You chose TestCases:" + str(strings)
            # calling create results sheet function to create sheet prior to execution
            # for string in strings:
            #     ue.create_results_sheet(string, pathname)
            print selections
            return selections
    except Exception as e:
        app.Destroy()
        print e
        print ins.stack()[0][3]


def select_sheet(sheet_list):
    try:
        app = wx.App()

        # frame = wx.Frame(None, -1, 'Select Sheet For Test Cases')
        # frame.SetSize(0, 0, 200, 50)

        # dlg = wx.MultiChoiceDialog(frame,
        #                            "Pick The TestCases To Be Executed","Acciom", test_list)

        dlg = SelectAll(None,"Pick The SheetName To View TestCases",
                                     "Acciom", choices=sheet_list)

        if dlg.ShowModal() == wx.ID_OK:
            strings = dlg.GetStringSelections()
            strings = strings[0]
            # strings = [sheet_list[x] for x in selections]
            print "You choose SheetName:" + str(strings)
            return strings
    except Exception as e:
        app.Destroy()
        print e
        print ins.stack()[0][3]


def select_columns(excludeColumns, testclass, target_df):
    try:
        if excludeColumns != '':
            column_list = list(target_df.columns.values)

            excludeColumns = re.split('\[|\]|\,|\\\'|\\"', excludeColumns)
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

            #frame = wx.Frame(None, -1, 'Select Tests')
            #frame.SetSize(0, 0, 200, 50)

            column_list = list(target_df.columns.values)
            dlg = SelectAll(None,"Pick The Columns For {}".format(testclass),
                                       "Acciom", choices=column_list)

            if dlg.ShowModal() == wx.ID_OK:
                selections = dlg.GetSelections()
                strings = [column_list[x] for x in selections]
                print "You chose:" + str(strings)
                return strings
    except Exception as e:
        app.Destroy()
        print e
        print ins.stack()[0][3]