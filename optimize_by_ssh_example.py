from multiprocessing import Process

import MySQLdb
from sshtunnel import SSHTunnelForwarder

import my_lib
import mysql_optimizer_lib

from operator import itemgetter

import time


def set_engine(table_data, connection):
    engine = 'aria'

    optimizer = mysql_optimizer_lib.Optimizer(
        MySQLdb.connect(
            **connection
        ))
    # print(table_index)
    optimizer.set_table_engine(
        table_data=table_data,
        engine=engine,
        pack_keys=0,
        transactoinal=0
    )

    return True


def set_compress(table_data, connection):
    optimizer = mysql_optimizer_lib.Optimizer(
        MySQLdb.connect(
            **connection
        ))

    optimizer.row_format(
        table_data=table_data,
        row_format='compressed'
    )

    return True


def remote_td(process_count=1):
    server = SSHTunnelForwarder(
        ('ttt', 8150),
        ssh_username="ttt",
        ssh_password="",
        remote_bind_address=('127.0.0.1', 3306),
        # local_bind_address=('127.0.0.1', 63306),
        compression=False
    )

    server.start()

    connection = dict(
        host='127.0.0.1',
        port=server.local_bind_port,
        db='ttt',
        user='dev',
        passwd='ttt',
        charset="utf8mb4",
        connect_timeout=30,
        autocommit=True,
        compress=True
    )

    optimizer = mysql_optimizer_lib.Optimizer(
        MySQLdb.connect(
            **connection
        ))

    # optimizer.table_status_rows = sorted(optimizer.table_status_rows, key=itemgetter('ENGINE'))

    list_ = optimizer.table_status_rows

    procs = dict()

    for index, row in enumerate(list_):

        def start():

            proc_ = Process(
                target=set_compress,
                args=(row, connection),
                daemon=True
            )

            procs[index] = proc_

            proc_.start()

        def wait():

            while True:
                if len(procs) >= process_count or index > len(list_) - process_count - 1:
                    for key, value in procs.items():
                        if value.is_alive():
                            time.sleep(0.5)

                        else:
                            if value.exitcode != 0:
                                print('Ошибка')

                                return False

                            procs.pop(key)

                            return True

                else:
                    return True

        # print(len(procs), row)

        start()

        if wait() is False:
            break

        print('OK ' + row['NAME'])

    server.close()

if __name__ == "__main__":
    remote_td()
