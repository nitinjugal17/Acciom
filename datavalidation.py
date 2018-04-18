
from openpyxl import load_workbook
import pandas as pd
import datetime
import inspect as ins
from os import path
import sys


def df_comparison(testcase_id, tc_id_data, source_df, target_df, pathname):
    try:
        book = load_workbook(pathname)
        writer = pd.ExcelWriter(pathname)
        writer.book = book
        if testcase_id in book.sheetnames:
            name = book.get_sheet_by_name(testcase_id)
            book.remove_sheet(name)

        if tc_id_data.get('targetPrimaryKey') != '':
            indexed_df = target_df.set_index(tc_id_data['targetPrimaryKey'])
        else:
            print '*#*#*#*#*#*#TARGET PRIMARY KEY INVALID*#*#*#*#*#*#'

        if tc_id_data.get('sourcePrimaryKey') != '':
            indexed_df2 = source_df.set_index(tc_id_data['sourcePrimaryKey'])
        else:
            print '*#*#*#*#*#*#SOURCE PRIMARY KEY INVALID*#*#*#*#*#*#'


        for column in indexed_df:
            if column in tc_id_data['targetColumn']:
                # moves the control back to the top of the loop
                continue
            else:
                indexed_df = indexed_df.drop(column, axis=1)
                # print 'dropped {}'.format(column)
        for column in indexed_df2:
            if column in tc_id_data['targetColumn']:
                # moves the control back to the top of the loop
                continue
            else:
                indexed_df2 = indexed_df2.drop(column, axis=1)
                # print 'dropped {}'.format(column)

        diff_panel = pd.Panel(dict(df1=indexed_df2, df2=indexed_df))

        # Applying the diff function
        diff_output = diff_panel.apply(report_diff, axis=0)

        # Flag all the changes
        diff_output['has_change'] = diff_output.apply(has_change, axis=1)

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
            print "No Mismatch Found, Verification Count as 0 == {}".format(len(output_reduce))
            return True
        else:
            print '#*#*#*#*#*#*#*#For {} , Number of Mismatch Found:{}'.format(testcase_id, len(output_reduce))
            writer.save()
            return False
    except Exception as e:
        print e
        print ins.stack()[0][3]
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return False


def report_diff(x):
    # diff function to show the changes in each field
    return x[0] if x[0] == x[1] else '{} ---> {}'.format(*x)


def has_change(row):
    # tell which rows have changes
    if "--->" in row.to_string():
        return "Y"
    else:
        return "N"
