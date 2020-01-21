from itertools import repeat

import time, decimal, operator


def split_list_to_chunks(l, n):
    # Разбивает лист на серии по несколько элементов
    for i in range(0, len(l), n):
        yield l[i:i + n]


def insert_bath(
        row_list,
        table_name,
        cursor,
        server_type='mysql',
        insert_mode: str = 'insert'
):
    if not row_list:
        return False

    assert server_type in ('mysql', 'sphinx'), 'Не опознан тип сервара базы для вставки'

    assert insert_mode in ('insert', 'replace', 'insert_ignore'), 'Не опознан режим вставки'

    sql_columns = tuple(row_list[0].keys())

    insert_values_sql_part = []
    insert_values = []

    for item in row_list:

        insert_value = list()

        for key, value in item.items():

            if value is None and server_type == 'sphinx':
                value = ''

            insert_value.append(value)

        insert_values_sql_part.append('(' + ','.join(repeat('%s', len(insert_value))) + ')')
        insert_values += insert_value

    if insert_mode == 'insert':
        sql_start = 'INSERT'

    elif insert_mode == 'replace':
        sql_start = 'REPLACE'

    elif insert_mode == 'insert_ignore':
        sql_start = 'INSERT IGNORE'

    else:
        raise ValueError('Не  найден  режим  вставки')

    sql = f'''
            {sql_start}  INTO  {table_name}
            ({','.join(sql_columns)})
            VALUES {','.join(insert_values_sql_part)}
    '''

    cursor.execute(sql, insert_values)

    return True


class ConsolePrint:

    def __init__(self):
        self.last_message_time = time.time()

    def print(self, message):
        current_time = time.time()

        print(f'{round_to_decimal(current_time - self.last_message_time, 2)}  {message}')

        self.last_message_time = current_time


def round_to_decimal(price, decimal_places=2):
    price = decimal.Decimal(price).quantize(decimal.Decimal(f'''1.{''.zfill(decimal_places)}'''))

    return price


def group_list_by_key_in_dict(
        list_: list or tuple,
        key_name: str,
        key_name_sort: str = None,
        sort_reverse: bool = False
):
    res_dict = dict()

    for item in list_:

        key = item[key_name]

        if key not in res_dict:
            res_dict[key] = [item]

        else:
            res_dict[key].append(item)

    if isinstance(key_name_sort, str):
        for key in res_dict.keys():
            res_dict[key].sort(key=operator.itemgetter(key_name_sort), reverse=sort_reverse)

    return res_dict
