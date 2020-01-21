import os
import re
import uuid
import time
from operator import itemgetter

import MySQLdb

from MySQLdb.connections import Connection

import my_lib


class Optimizer:
    cursor = None
    start_time = None
    db = None

    def __init__(self, mysql_db: Connection):

        self.start_time = time.time()

        self.db = mysql_db

        self.cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
        self.cursor.execute('show table status', [])

        self.table_status_rows = self.cursor.fetchall()
        self.table_status_rows = self.upper_keys_and_values_in_list_dict(self.table_status_rows)

        self.table_status_rows = sorted(self.table_status_rows, key=itemgetter('ROWS'))

    def get_server_status_variable(self, variable_name):
        self.cursor.execute(''' 
               SHOW global STATUS  LIKE  %s
               ''', [variable_name])

        res1 = self.cursor.fetchone()

        if not res1:
            return None

        res2 = self.upper_keys_and_values_in_dict(res1)

        return res2['VALUE']

    def get_table_info(self, table_name):
        self.cursor.execute(''' 
               show table status where Name=%s
               ''', [table_name])

        table_info = self.cursor.fetchone()

        table_info = self.upper_keys_and_values_in_dict(table_info)

        return table_info

    def upper_keys_and_values_in_dict(self, dict_):
        result = dict()

        for key, value in dict_.items():
            if isinstance(key, str):
                key = key.upper()

            if isinstance(value, str) and key != 'NAME':
                value = value.upper()

            result.update({key: value})

        return result

    def upper_keys_and_values_in_list_dict(self, list_):
        result = list()

        for item in list_:
            result.append(self.upper_keys_and_values_in_dict(item))

        return tuple(result)

    def pack_keys(self, table_name, option):
        option = str(option).upper()

        if re.match('[10]', option) is None:
            raise ValueError('Ошибка параметров функции')

        if str(self.get_table_info(table_name)['CREATE_OPTIONS']).find('PACK_KEYS=' + option) == -1:
            self.cursor.execute(f'ALTER TABLE `{table_name}` PACK_KEYS = {option}')

    def row_format(self, table_name, option):
        option = str(option).upper()

        if str(self.get_table_info(table_name)['CREATE_OPTIONS']).find('ROW_FORMAT=' + option) == -1:
            self.cursor.execute(f'ALTER TABLE `{table_name}` ROW_FORMAT = {option}')

    def aria_transactional(self, table_name, option):
        option = str(option)

        if re.match('[10]', option) is None:
            raise ValueError('Ошибка параметров функции')

        if str(self.get_table_info(table_name)['CREATE_OPTIONS']).find('TRANSACTIONAL=' + option) == -1:
            self.cursor.execute(f'ALTER TABLE `{table_name}` TRANSACTIONAL = {option}')

    def aria_page_checksum(self, table_name, option):
        option = str(option)

        if re.match('[10]', option) is None:
            raise ValueError('Ошибка параметров функции')

        if str(self.get_table_info(table_name)['CREATE_OPTIONS']).find(' PAGE_CHECKSUM=' + option) == -1:
            self.cursor.execute(f'ALTER TABLE `{table_name}` PAGE_CHECKSUM={option}')

    def set_table_engine(self, table_data, engine, block_rows_count=5000, pack_keys=0, transactoinal=0):

        con = my_lib.ConsolePrint()

        engine = str(engine).upper()

        if table_data['ENGINE'] == engine:
            return

        table_name = table_data['NAME']

        con.print(table_name)

        if engine in ['MYISAM', 'INNODB', 'ARIA']:
            self.cursor.execute(f'ALTER TABLE `{table_name}` ENGINE = {engine} PACK_KEYS = {pack_keys} TRANSACTIONAL = {transactoinal}')

            return

        tmp_table = f'{table_name}__tmp_convert_850d68'

        self.cursor.execute(f'drop table if exists {tmp_table}')
        self.cursor.execute(f'''create table {tmp_table} like {table_name}''')
        self.cursor.execute(f'''alter table {tmp_table} engine {engine}''')

        self.cursor.execute(f'''handler {table_name} open''')

        row_count = 0

        while True:
            self.cursor.execute(f'handler {table_name} read next  limit {block_rows_count}')

            res = self.cursor.fetchall()

            if not res:
                break

            my_lib.insert_bath(
                row_list=res,
                cursor=self.cursor,
                table_name=tmp_table
            )

            row_count += block_rows_count

            con.print(row_count)

        self.cursor.execute(f'handler {table_name} close')

        self.cursor.execute(f'''
                            rename table  
                            `{table_name}`
                            to 
                            `{table_name}_backup`  ,

                            `{tmp_table}`
                            to 
                            `{table_name}`  ,

                            `{table_name}_backup`
                            to 
                            `{tmp_table}`
                      ''')

        self.cursor.execute(f'drop table if exists {tmp_table}')

    def table_checksum(self, table_name, option):
        option = str(option)

        if re.match('[10]', option) is None:
            raise ValueError('Ошибка параметров функции')

        if str(self.get_table_info(table_name)['CREATE_OPTIONS']).find(' CHECKSUM=' + option) == -1:
            self.cursor.execute(f'ALTER TABLE `{table_name}` CHECKSUM = {option}')

    def table_charset(self, table_name, option):
        option = str(option).upper()

        if str(self.get_table_info(table_name)['COLLATION']).split('_')[0] != option:
            self.cursor.execute(f'ALTER TABLE `{table_name}` CHARACTER SET = {option} COLLATE {option}_general_ci')

            print('table_charset ' + option)

    def proc_set_checksum(self):
        for row in self.table_status_rows:
            print(f'''Таблица {row['NAME']}''')

            self.table_checksum(row['NAME'], 1)

        print('Ok')

    def proc_compress_innodb(self, ignore_tables: list):
        console = my_lib.ConsolePrint()

        for row in self.table_status_rows:
            console.print(f'''Таблица {row['NAME']}''')

            last_engine = self.get_table_info(row['NAME'])['ENGINE']
            last_row_format = self.get_table_info(row['NAME'])['ROW_FORMAT']
            last_data_length = self.get_table_info(row['NAME'])['DATA_LENGTH']

            if (
                    last_row_format != 'COMPRESSED' and
                    # last_engine != 'INNODB' and
                    row['NAME'] not in ignore_tables
            ):
                self.cursor.execute(f'''
                        ALTER TABLE `{row['NAME']}`
                            ENGINE=InnoDB,
                            ROW_FORMAT=COMPRESSED;                       
                        ''')

                time.sleep(15)

                current_data_length = self.get_table_info(row['NAME'])['DATA_LENGTH']
                current_engine = self.get_table_info(row['NAME'])['ENGINE']
                # current_row_format = self.get_table_info(row['NAME'])['ROW_FORMAT']

                if current_data_length >= last_data_length:
                    console.print(
                        f'Сжатый размер {current_data_length} '
                        f'больше исходного {last_data_length}, откат обратно'
                    )

                    if current_engine != last_engine:
                        self.set_table_engine(row['NAME'], last_engine)

                    if self.get_table_info(row['NAME'])['ROW_FORMAT'] != last_row_format:
                        self.row_format(row['NAME'], last_row_format)
                else:
                    console.print(f"Ok До {last_data_length}  После {current_data_length}")

    def proc_row_format(self, row_format):
        console = my_lib.ConsolePrint()

        for row in self.table_status_rows:
            console.print(f'''Таблица {row['NAME']}''')

            self.row_format(row['NAME'], row_format)