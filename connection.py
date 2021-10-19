# import MySQLdb as mdb
from datetime import date, datetime, timedelta
import sys
import pyodbc


class mysql_db_connection(object):
    host = ''
    user = ''
    password = ''
    database = ''

    def __init__(self, config):
        self.host = config['host']
        self.user = config['user']
        self.password = config.get('password', None)
        config['use_unicode'] = True
        config['charset'] = "utf8"

        if 'database' in config.keys():
            config['db'] = config.pop('database')

        if 'password' in config.keys():
            config['passwd'] = config.pop('password')

        self.password = config.get('passwd', None)
        self.database = config.get('db', None)
        self.opts = config
        self.db_connection = self.open_db_conn()
        self.v_col_info = []
        self.v_last_error = ""

    def open_db_conn(self):
        try:
            return pyodbc.connect(**self.opts)
        except pyodbc.Error as e:
            raise e
        return None

    def escape_str(self, str):
        return pyodbc.escape_string(str)

    def get_results(self, sql_query, sql_commit=False):
        try:
            select_cursor = self.db_connection.cursor()
            select_cursor.execute(sql_query)
            returned_data = select_cursor.fetchall()
            self.v_col_info = select_cursor.description
            select_cursor.close()
            if sql_commit:
                self.db_connection.commit()
            return returned_data
        except Exception as e:
            if sql_commit:
                self.db_connection.rollback()
            self.handle_error(e, True)
            return False

    def get_row(self, sql_query, sql_commit=False):
        try:
            select_cursor = self.db_connection.cursor()
            select_cursor.execute(sql_query)
            returned_data = select_cursor.fetchone()
            self.v_col_info = select_cursor.description
            select_cursor.close()
            if sql_commit:
                self.db_connection.commit()
            return returned_data
        except Exception as e:
            if sql_commit:
                self.db_connection.rollback()
            self.handle_error(e, True)
            return False

    def get_results_multi_execute(self, sql_query, sql_commit=False):
        try:
            select_cursor = self.db_connection.cursor()
            select_cursor.executemany(sql_query)
            returned_data = select_cursor.fetchall()
            self.v_col_info = select_cursor.description
            select_cursor.close()
            if sql_commit:
                self.db_connection.commit()
            return returned_data
        except Exception as e:
            if sql_commit:
                self.db_connection.rollback()
            self.handle_error(e, True)
            return False
    def get_results_generator(self, sql_query, p_block=500, sql_commit=False):
        try:
            select_cursor = self.db_connection.cursor()
            select_cursor.execute(sql_query)

            self.v_col_info = select_cursor.description
            while select_cursor.rows:
                returned_data = select_cursor.fetchmany(p_block)
                yield returned_data
            select_cursor.close()
            if sql_commit:
                self.db_connection.commit()
            yield returned_data
        except Exception as e:
            if sql_commit:
                self.db_connection.rollback()
            self.handle_error(e, True)
            yield False

    def get_last_error(self):
        return self.v_last_error

    def get_col_info(self):
        col_info = []
        for a, b, c, d, e, f, g in self.v_col_info:
            col_info.append([a, b])
        return col_info

    # def get_columns(self):
    #     v_columns = []
    #     for a, b, c, d, e, f, g in self.v_col_info:
    #         v_columns.append(a)
    #     return v_columns

    # def insert_many(self, p_table, p_col_info, p_data, p_schema=None, p_insert='INSERT', p_update=None):
    #     from collections import OrderedDict
    #     try:
    #         if p_schema is None:
    #             p_schema = self.database
    #
    #         v_replace = OrderedDict()
    #         v_replace['schema'] = p_schema
    #         v_replace['table'] = p_table
    #         v_replace['col'] = ''
    #         v_replace['val'] = ''
    #
    #         v_replace['col'] = '(`%s`)' % ('`,`'.join(p_col_info),)
    #
    #         p_hold = []
    #         for i in range(0, len(p_col_info)):
    #             p_hold.append('%s')
    #
    #         v_replace['val'] = '(\'%s\')' % ("','".join(p_hold),)
    #         v_format = tuple(v_replace.values())
    #
    #         v_insert_query = """
    #                     INSERT INTO %s.%s
    #                         %s
    #                     VALUES
    #                         %s
    #                     """ % v_format
    #
    #         insert_cursor = self.db_connection.cursor()
    #
    #         p_mapped_data = []
    #
    #         for row in p_data:
    #             tmp = []
    #             if type(row) == dict:
    #                 for k in p_col_info:
    #                     tmp.append(str(row[k]))
    #             else:
    #                 for k in row:
    #                     tmp.append(str(k))
    #             p_mapped_data.append(tmp)
    #
    #         debug(v_insert_query, True)
    #
    #         affected = insert_cursor.executemany(v_insert_query, p_mapped_data)
    #         self.db_connection.commit()
    #         return affected
    #
    #     except Exception as e:
    #         raise e
    #         return False
    #     return False

    # def get_results_dict(self, sql_query):
    #     try:
    #         select_cursor = self.db_connection.cursor(cursorclass=mdb.cursors.DictCursor)
    #         select_cursor.execute(sql_query)
    #         returned_data = select_cursor.fetchall()
    #         self.v_col_info = select_cursor.description
    #         select_cursor.close()
    #         return returned_data
    #     except Exception as e:
    #         self.handle_error(e, True)
    #         return False
    #     return False

    # def insert_dict(self, table, data, p_update = None):
    #     """
    #
    #     """
    #     query = 'INSERT INTO ' + table
    #     cols = data[0].keys()
    #     table_colstring= ' (`' + '`,`'.join(cols) + '`) VALUES '
    #     v_data = []
    #     for row in data:
    #         row_info = []
    #         for v in row.values():
    #             row_info.append(str(v))
    #
    #         v_data.append("('" + "','".join(row_info ) + "')")
    #
    #     datastring = ','.join(v_data)
    #     q = query + table_colstring + datastring
    #     self.execute(q)

    # def insert_dict_upd(self, table, data, update=None, block=500):
    #     """
    #     insert on duplicate update
    #     """
    #     from collections import OrderedDict
    #     insert = 'INSERT'
    #     sort = []
    #     cols = data[0].keys()
    #
    #     for row in data:
    #         tmp = OrderedDict()
    #         for k in cols:
    #             tmp[k] = str(row[k])
    #         sort.append(tmp)
    #
    #     data = sort
    #     query = '%s INTO %s ' % (insert,table)
    #
    #     table_colstring= ' (`' + '`,`'.join(cols) + '`) VALUES '
    #     upd_str = ''
    #     if update is not None:
    #         if type(update) in (tuple, list):
    #             tmp = []
    #             for r in update:
    #                 if r in cols:
    #                     tmp.append('`%s` = VALUES(`%s`)' % (r, r))
    #             if len(tmp) > 0:
    #                 upd_str = ' ON DUPLICATE KEY UPDATE ' + ', '.join(tmp)
    #         if type(update) == dict:
    #             tmp = []
    #             for k, v in update.items():
    #                 tmp.append("`%s` = '%s'" % (k, v))
    #             if len(tmp) > 0:
    #                 upd_str = ' ON DUPLICATE KEY UPDATE ' + ', '.join(tmp)
    #
    #     while len(data) > 0:
    #         v_data = []
    #         x = 0
    #         datastring = ''
    #
    #         while len(data) > 0 and x < block:
    #             row = data.pop(0)
    #             if type(row) in (OrderedDict, dict):
    #                 row = row.values()
    #             v_data.append("('" + "','".join(row) + "')")
    #
    #             datastring = ','.join(v_data)
    #             x += 1
    #             print(datastring)
    #         print('LEFT %d rows' % len(data))
    #         q = query + table_colstring + datastring + upd_str
    #         ##run the query
    #         self.execute(q)

    # def insert_ignore_dict(self, p_table, p_data, p_update=None):
    #     """
    #
    #     """
    #     self.insert_dict( p_table, p_data, p_insert='INSERT IGNORE', p_update=p_update)
    #
    # def replace_dict(self, p_table, p_data, p_update=None):
    #     """
    #
    #     """
    #     self.insert_dict( p_table, p_data, p_insert='REPLACE', p_update=p_update)

    # def run_procedure(self, procedure_name, args):
    #     try:
    #         proc_cursor = self.db_connection.cursor(cursorclass=pyodbc.cursors.DictCursor)
    #         return_args = proc_cursor.callproc(procedure_name, args)
    #         proc_cursor.close()
    #         return return_args
    #         self.db_connection.commit()
    #     except Exception as e:
    #         self.handle_error(e, True)
    #         self.db_connection.rollback()
    #         return False
    #     return True


    # def handle_error(self, p_error,  p_log):
    #     print('mysql_db_connection:error')
    #     self.v_last_error = p_error
    #     debug(p_error)
        #self.db_connection.defaulterrorhandler(self, none, type(p_error), p_error)

    # def execute(self, sql_query, sql_commit=False):
    #     try:
    #         exec_cursor = self.db_connection.cursor()
    #         exec_cursor.execute(sql_query)
    #         exec_cursor.close()
    #         self.db_connection.commit()
    #     except Exception as e:
    #         self.handle_error(e, True)
    #         self.db_connection.rollback()
    #         raise e
    #     return True

    # def get_insert_id(self):
    #     return self.get_results("SELECT LAST_INSERT_ID();")[0][0]


