
from openpyxl import load_workbook
import pandas as pd
import datetime
from os import path
import inspect as ins


def check_count(testcase_id, source_df, target_df, pathname):
    try:
        source_column = len(source_df.columns.values.tolist())
        source_row = len(source_df.index.values.tolist())
        target_column = len(target_df.columns.values.tolist())
        target_row = len(target_df.index.values.tolist())

        book = load_workbook(pathname)
        writer = pd.ExcelWriter(pathname)
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
    except Exception as e:
        print e
        print ins.stack()[0][3]
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return False
