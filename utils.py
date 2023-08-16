from datetime import datetime

import xlsxwriter
from functools import wraps


def write_data(last_run, data, keys):
    # data = 'test data'
    if not data:
        return

    file_mark = last_run.replace(' ', '_')
    file_mark = file_mark.replace(':', '-')
    workbook = xlsxwriter.Workbook(f'report_{file_mark}.xlsx')
    worksheet = workbook.add_worksheet(f'Отчёт на {file_mark}')

    # заводим "шапку" документа
    for i in range(len(keys)):
        worksheet.write(0, i, keys[i])

    # пишем данные
    for i in range(1, len(data)+1):
        row = data[i-1]
        for j in range(0, len(keys)):
            worksheet.write(i, j, str(row[j]))

    workbook.close()
