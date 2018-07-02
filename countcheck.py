
from openpyxl import load_workbook
import pandas as pd
import datetime
from os import path
import inspect as ins
import sys


def check_count(testcase_id, source_df, target_df, pathname):
    try:
        source_column = source_df.columns.values.tolist()
        source_row = source_df.index.values.tolist()
        target_column = target_df.columns.values.tolist()
        target_row = target_df.index.values.tolist()

        book = load_workbook(pathname)
        writer = pd.ExcelWriter(pathname)
        writer.book = book

        #sheet = book.get_sheet_by_name(str(testcase_id))
        sheet = book[str(testcase_id)]
        max_index = sheet.max_row
        # Make the text of the cell bold and italic
        # cell = sheet['A{}'.format(max_index + 1)]
        sheet['A{}'.format(max_index + 2)].value = 'Column'
        sheet['A{}'.format(max_index + 3)].value = 'Row'

        sheet['B{}'.format(max_index + 1)].value = 'Source'
        sheet['C{}'.format(max_index + 1)].value = 'Target'
        sheet['D{}'.format(max_index + 1)].value = 'Result'

        sheet['B{}'.format(max_index + 2)].value = len(source_column)
        sheet['C{}'.format(max_index + 2)].value = len(target_column)
        sheet['B{}'.format(max_index + 3)].value = len(source_row)
        sheet['C{}'.format(max_index + 3)].value = len(target_row)

        if source_column == target_column:
            sheet['D{}'.format(max_index + 2)].value = 'PASS'
        else:
            sheet['D{}'.format(max_index + 2)].value = 'FAIL'
        if source_row == target_row:
            sheet['D{}'.format(max_index + 3)].value = 'PASS'
        else:
            sheet['D{}'.format(max_index + 3)].value = 'FAIL'

        sheet['B5'].value = 'SOURCE COLUMNS'
        max_index = sheet.max_row
        for column in range(len(source_column)):
            sheet['B{}'.format(max_index + column)].value = source_column[column]

        sheet['C5'].value = 'TARGET COLUMN'
        for index in range(len(target_column)):
            sheet['C{}'.format(max_index + index)].value = target_column[index]

        max_index = sheet.max_row
        sheet['A{}'.format(max_index + 1)].value = 'Execution TimeStamp'
        sheet['B{}'.format(max_index + 1)].value = datetime.datetime.now()
        writer.save()

        return True
    except Exception as e:
        print e
        print ins.stack()[0][3]
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return False
