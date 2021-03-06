# GPL3 License

# Original work Copyright (c) 2019 Rene Bakker
# Modified work Copyright 2020 Morten Eek

# Based on an idea by Fredrik Lundh (effbot.org/zone/tkinter-autoscrollbar.htm)
# adapted to support all layouts

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import sys
import os
from pathlib import Path
from collections import OrderedDict
from decimal import Decimal
from jpype import JPackage, startJVM
from jaydebeapi import Cursor, Error, DatabaseError, connect


# class_path = '/home/bba/bin/PWCode/bin/vendor/sqlwbj/sqlworkbench.jar'
# startJVM(str(Path(__file__).parents[1]) + '/vendor/linux/jre/bin/java', "-ea", "-Djava.class.path=" + class_path)

#from .config_parser import JDBC_DRIVERS, JAR_FILES, parse_login, parse_dummy_login
# from .exceptions import DriverNotFoundException, SQLExcecuteException, CommitException
from .runtime_statistics import RuntimeStatistics
from .utils import *

PARENT_CONNECTION = '_pwcode_jdbc'

# Handled column types
COLUMN_TYPE_NUMBER = 'number'
COLUMN_TYPE_FLOAT = 'float'
COLUMN_TYPE_DATE = 'date'
COLUMN_TYPE_STRING = 'str'
COLUMN_TYPE_BINARY = "byte"

DEC_ZERO = Decimal(0.0)

JAVA_STRING = None


def default_cursor(default_result):
    """
    Decorator: addes check on the cursor.
    The function will return the default_result if the the specified cursor does not exist
    or current cursor is not defined.
    @param default_result: Any - the default value the function should return if the cursor is not found
    @return: decorated function

    @see http://www.artima.com/weblogs/viewpost.jsp?thread=240845
    """

    def wrap(func):
        def func_wrapper(*args, **kwargs):
            if len(args) < 1:
                raise LookupError('Illegal function for default_cursor decorator')
            self = args[0]
            argl = list(args)
            cursor = kwargs.get('cursor', None)
            if isinstance(cursor, Cursor):
                del kwargs['cursor']
            else:
                if (len(argl) < 2) or (argl[1] is None):
                    cursor = self.current
                elif isinstance(argl[1], Cursor):
                    cursor = argl[1]

            if isinstance(cursor, Cursor):
                if cursor not in self.cursors:
                    raise LookupError('Specified cursor not found in this instance.')
            elif cursor is not None:
                raise ValueError('Illegal cursor specifier.')

            if cursor is None:
                return default_result
            elif len(argl) == 1:
                argl.append(cursor)
            else:
                argl[1] = cursor

            return func(*tuple(argl), **kwargs)

        return func_wrapper

    return wrap


def get_columns_of_cursor(cursor: Cursor) -> OrderedDict:
    """
    Retrieve the column information of the specified cursor
    @param cursor: Cursor
    @return: OrderedDict of columns - key: name of the column, value: type of the column
    """

    upper_case = False
    if isinstance(cursor, Cursor):
        if hasattr(cursor, PARENT_CONNECTION):
            upper_case = getattr(cursor, PARENT_CONNECTION).upper_case
    else:
        raise TypeError('cursor must be of type Cursor, found: ' + type(cursor).__name__)

    columns = OrderedDict()
    if cursor.description is not None:
        for x in range(cursor._meta.getColumnCount()):
            name = cursor._meta.getColumnLabel(x + 1)
            if upper_case:
                name = name.upper()

            ctype = cursor.description[x][1]
            if ctype is None:
                columns[name] = COLUMN_TYPE_STRING
            elif len({'INTEGER', 'DECIMAL', 'NUMERIC'}.intersection(ctype.values)) > 0:
                columns[name] = COLUMN_TYPE_NUMBER
            elif len({'FLOAT', 'REAL', 'DOUBLE'}.intersection(ctype.values)) > 0:
                columns[name] = COLUMN_TYPE_FLOAT
            elif 'TIMESTAMP' in ctype.values:
                columns[name] = COLUMN_TYPE_DATE
            else:
                columns[name] = COLUMN_TYPE_STRING
    return columns


class DataTransformer:
    """
        Row types returned by jaydebeapi are not always of a python compatible type.
        This transformer class makes correctons.
    """

    def __init__(self, cursor: Cursor, return_type=tuple, upper_case: bool = True, include_none: bool = False):
        """
        Instantiate a DataTransformer

        @param cursor: Cursor containing query data.
        @param return_type: (optional) return type of the transformation. May be list, tuple (default), dict, or
            OrderedDict (see collections)
        @param upper_case: bool - transform column names in upper case (defaults to True)
        @param include_none: bool - include None values in dictionary return types. Defaults to False
        @return DataTransformer

        @raise ValueError if the cursor has no data
        @raise TypeError on a wrong cursor type, or wrong return type
        """
        global JAVA_STRING

        if not isinstance(cursor, Cursor):
            raise TypeError('Variable for the DataTransformer must be a Cursor. Found: ' + type(cursor).__name__)
        elif cursor.description is None:
            raise ValueError('Cannot create a DataTransformer on a cursor without data.')

        expected_types = [list, tuple, dict, OrderedDict]
        if return_type not in expected_types:
            str_types = [str(t).split("'")[1] for t in expected_types]
            raise TypeError('Specified return type must me one of: %s. Found: %s' % (
                ', '.join(str_types), type(return_type).__name__))
        self.return_type = return_type
        self.include_none = verified_boolean(include_none)

        upper_case = verified_boolean(upper_case)

        columns = []
        column_types = []
        for x in range(cursor._meta.getColumnCount()):
            name = cursor._meta.getColumnLabel(x + 1)
            if upper_case:
                name = name.upper()
            columns.append(name)
            type_def = cursor.description[x][1]
            if type_def is None:
                column_types.append(COLUMN_TYPE_STRING)
            elif len({'INTEGER', 'DECIMAL', 'NUMERIC'}.intersection(type_def.values)) > 0:
                column_types.append(COLUMN_TYPE_NUMBER)
            elif len({'FLOAT', 'REAL', 'DOUBLE'}.intersection(type_def.values)) > 0:
                column_types.append(COLUMN_TYPE_FLOAT)
            elif 'TIMESTAMP' in type_def.values:
                column_types.append(COLUMN_TYPE_DATE)
            else:
                column_types.append(COLUMN_TYPE_STRING)
        self.columns = tuple(columns)
        self.transformer = column_types
        self.nr_of_columns = len(columns)

        if JAVA_STRING is None:
            # JVM must have started for this
            JAVA_STRING = JPackage('java').lang.String

    @staticmethod
    def byte_array_to_bytes(array):
        return bytes([(lambda i: (256 + i) if i < 0 else i)(b) for b in array])

    @staticmethod
    def default_transformer(v):
        if isinstance(v, str):
            # Bugfix: jpype for some multi-byte characters parses the surrogate unicode escape string
            #         most notably 4-byte utf-8 for emoji
            return v.encode('utf-16', errors='surrogatepass').decode('utf-16')
        if type(v) == 'java.lang.String':
            return v.getBytes().decode()
        else:
            return v

    def oracle_lob_to_bytes(self, lob):
        # print(type(lob).__name__)
        return self.byte_array_to_bytes(lob.getBytes(1, lob.length()))

    def oracle_clob(self, clob):
        return clob.stringValue()

    @staticmethod
    def parse_number(number):
        if isinstance(number, int):
            return number
        else:
            dval = Decimal(str(number))
            if (dval % 1) == DEC_ZERO:
                return int(number)
            else:
                return dval

    def __call__(self, row):
        """
        Transform a row of data
        @param row: list or tuple
        @return: the transformed row in the return type specified when the class was instantiated
        """
        if row is None:
            return None
        try:
            row_length = len(row)
        except TypeError:
            row = [row]
            row_length = 1
        if row_length != self.nr_of_columns:
            raise ValueError('Invalid row. Expected %d elements but found %d.' % (self.nr_of_columns, row_length))
        if row_length == 0:
            return self.return_type()

        values = []
        for x in range(row_length):
            value = row[x]
            if value is None:
                values.append(None)
                continue

            func = self.transformer[x]
            if isinstance(func, str):
                # first time use
                vtype = type(value).__name__
                if vtype == 'oracle.sql.BLOB':
                    self.transformer[x] = self.oracle_lob_to_bytes
                elif vtype == 'oracle.sql.CLOB':
                    self.transformer[x] = self.oracle_clob
                elif vtype.startswith('java') or vtype.startswith('oracle'):
                    self.transformer[x] = (lambda v: v.toString())
                elif vtype == 'byte[]':
                    self.transformer[x] = self.byte_array_to_bytes
                elif func == COLUMN_TYPE_FLOAT:
                    self.transformer[x] = (lambda v: v if isinstance(v, float) else float(v))
                elif func == COLUMN_TYPE_NUMBER:
                    self.transformer[x] = self.parse_number
                else:
                    self.transformer[x] = self.default_transformer
                    #(lambda v: v)
                func = self.transformer[x]

            parse_exception = None
            try:
                values.append(func(value))
            except Exception as e:
                print('ERROR - cannot parse {}: {}'.format(value, str(e)))
                parse_exception = e
            if parse_exception is not None:
                raise parse_exception
        if self.return_type == list:
            return values
        elif self.return_type == tuple:
            return tuple(values)
        else:
            dd = self.return_type()
            for x in range(row_length):
                if self.include_none or (values[x] is not None):
                    # noinspection PyUnresolvedReferences
                    dd[self.columns[x]] = values[x]
            return dd


class DummyJdbc:
    """
    Dummy JDBC connection.
    Only stores configuration parameters of the connection, there is not real connection
    """

    def __init__(self, login_or_drivertype: str, upper_case=True):
        self.login = 'nobody'
        self.upper_case = upper_case
        self.type, self.always_escape = parse_dummy_login(login_or_drivertype)

    def commit(self):
        pass

    def rollback(self):
        pass

    def execute(self, sql, parametes=None, cursor=None):
        return cursor

    def get_int(self, sql, parameters=None):
        return 0


class Jdbc:
    """
    Jdbc connection manager.

    The class loads general connection settings from a configuration file (see below).

    """

    def __init__(self, url, usr, pwd, db_name, db_schema, driver_jar, driver_class, auto_commit=False, upper_case=False):
        """
        Init the jdbc connection.
        @param auto_commit: bool - auto-commit each sql statement. Defaults to False
                                   (changes are only committed with the jdbc.commit() command)
        @param upper_case: bool
        @raises (ConnectionError,DriverNotFoundException) if the connection could not be established
        """
        self.url = url
        self.usr = usr
        self.pwd = pwd
        self.db_name = db_name
        self.db_schema = db_schema
        self.auto_commit = verified_boolean(auto_commit)
        self.upper_case = verified_boolean(upper_case)
        self.connection = None
        self.driver_jar = driver_jar
        self.driver_class = driver_class

        connection_error = None
        try:

            # print(JDBC_DRIVERS['class'])
            # print(JAR_FILES)
            # print(JDBC_DRIVERS)
            # print(self.url)
            # test = JDBC_DRIVERS[self.type]['class']
            # print(test)

            self.connection = connect(self.driver_class, self.url, [self.usr, self.pwd], self.driver_jar,)
            self.connection.jconn.setAutoCommit(auto_commit)
        except Exception as error:
            error_msg = str(error)
            print(error_msg)
        #     if ('Class' in error_msg) and ('not found' in error_msg):
        #         connection_error = DriverNotFoundException(error_msg)
        #     elif 'Could not create connection to database server' in error_msg:
        #         connection_error = ConnectionError('Failed to connect to: ' + self.url)
        #     else:
        #         print('test')
        #         connection_error = ConnectionError(error_msg)
        #     if ':' in error_msg:
        #         error_msg = error_msg.split(':', 1)[-1]
        #     print('ERROR - jdbc connection failed: ' + error_msg, file=sys.stderr)

        # if connection_error is not None:
        #     raise connection_error

        # for statistics
        self.statistics = RuntimeStatistics()

        # cursor handling
        self.counter = 0
        self.cursors = []
        self.current = None

    def __del__(self):
        if self.connection:
            for cursor in self.cursors:
                try:
                    cursor.close()
                except Exception:
                    pass
            try:
                self.connection.close()
            except Exception:
                pass

    @default_cursor(False)
    def close(self, cursor=None):
        """
        Closes the specified cursor. Use the current if not specified.
        @param cursor: Cursor|str - cursor or id of the cursor
        @return bool: true on success
        """

        try:
            cursor.close()
            close_ok = True
        except Error:
            close_ok = False
        else:
            self.cursors.remove(cursor)
            if cursor == self.current:
                if len(self.cursors) > 0:
                    self.current = self.cursors[-1]
                else:
                    self.current = None
            try:
                # for garbage collection
                del cursor
            except Exception:
                pass
        return close_ok

    def execute(self, sql: str, parameters: (list, tuple) = None, cursor: object = None) -> Cursor:
        """
        Execute a query
        @param sql: str query to execute
        @param parameters: list of parameters specified in the sql query. May also be None (no parameters), or
            a list of lists (execute many)
        @param cursor: to use for exection. Create a new one if None (default)
        @return: Cursor of the execution

        @raise SQLExecutionError on an execution exception
        """

        def string2java_string(sql_or_list):
            # Bugfix: 4-byte UTF-8 is not parsed correctly into jpype

            global JAVA_STRING
            if JAVA_STRING is None:
                # JVM must have started for this
                JAVA_STRING = JPackage('java').lang.String

            if sql_or_list is None:
                return None
            elif isinstance(sql_or_list, str):
                return JAVA_STRING(sql_or_list.encode(), 'UTF8')
            elif isinstance(sql_or_list, (list, tuple)):
                parms = []
                for p in sql_or_list:
                    if isinstance(p, str):
                        parms.append(JAVA_STRING(p.encode(), 'UTF8'))
                    else:
                        parms.append(p)
                return parms

        if is_empty(sql):
            raise ValueError('Query string (sql) may not be empty.')
        elif not isinstance(sql, str):
            raise TypeError('Query (sql) must be a string.')

        if (cursor is not None) and (cursor not in self.cursors):
            cursor = None
        if cursor is None:
            self.counter += 1
            cursor = self.connection.cursor()
            self.cursors.append(cursor)
        self.current = cursor

        while sql.strip().endswith(';'):
            sql = sql.strip()[:-1]
        error_message = None
        with self.statistics as stt:
            try:
                if isinstance(parameters, (list, tuple)) and (len(parameters) > 0) and (
                        isinstance(parameters[0], (list, tuple, dict))):
                    stt.add_exec_count(len(parameters))
                    cursor.executemany(sql, [string2java_string(p) for p in parameters])
                else:
                    stt.add_exec_count()
                    if parameters is None:
                        cursor.execute(string2java_string(sql), None)
                    else:
                        cursor.execute(sql, string2java_string(parameters))
            except Exception as execute_exception:
                self.close(cursor)
                error_message = str(execute_exception)
                for prefix in ['java.sql.']:
                    if error_message.startswith(prefix):
                        error_message = error_message[len(prefix):]
            if error_message is not None:
                print(sql, file=sys.stderr)
                if isinstance(parameters, (list, tuple)):
                    print(parameters, file=sys.stderr)
                raise SQLExcecuteException(error_message)

        if not hasattr(cursor, PARENT_CONNECTION):
            # mark myself for column retrieval, see get_columns_of_cursor()
            setattr(cursor, PARENT_CONNECTION, self)
        return cursor

    @default_cursor(None)
    def get_cursor(self, cursor=None):
        """
        Get the current cursor if not specified. Return None if the provided cursor
        is either closed or not handled by the current instance
        @param cursor: Cursor - the cursor of interest
        @return: Cursor|None
        """
        return cursor

    @default_cursor([])
    def get_columns(self, cursor=None) -> OrderedDict:
        """
        Get the column associated to the cursor
        @param cursor: cursor to query. Current if not specified
        @return: OrderedDict of the defined columns:
         - key name of the column (in uppercase if self.upper_case=True)
         - value type of the column: one of the COLUMN_TYPE defined above
        """
        return get_columns_of_cursor(cursor)

    @default_cursor(None)
    def get_data(self, cursor: Cursor = None, return_type=tuple,
                 include_none=False, max_rows: int = 0, array_size: int = 1000):
        """
        An iterator using fetchmany to keep the memory usage reasonalble
        @param cursor: Cursor to query, use current if not specified
        @param return_type: return type of rows. May be list, tuple (default), dict, or OrderedDict
        @param include_none: bool return None values in dictionaries, if True. Defaults to False
        @param max_rows: int maximum number of rows to return before closing the cursor. Negative or zero implies
            all rows
        @param array_size: int - the buffer size
        @return: iterator
        """
        if (not isinstance(array_size, int)) or array_size < 1:
            array_size = 1
        if (not isinstance(max_rows, int)) or max_rows < 0:
            max_rows = 0

        batch_nr = 0
        row_count = 0
        transformer = DataTransformer(cursor,
                                      return_type=return_type, upper_case=self.upper_case, include_none=include_none)
        while True:
            batch_nr += 1
            fetch_error = None
            results = []
            try:
                results = cursor.fetchmany(array_size)
            except Error as error:
                fetch_error = error

            if fetch_error is not None:
                print('Fetch error in batch %d of size %d.' % (batch_nr, array_size), file=sys.stderr)
                error_msg = str(fetch_error)
                print(error_msg, file=sys.stderr)
                raise SQLExcecuteException('Failed to fetch data in batch %d: %s' % (batch_nr, error_msg))

            if len(results) == 0:
                self.close(cursor)
                break
            for result in results:
                row_count += 1
                yield transformer(result)
                if (max_rows > 0) and (row_count >= max_rows):
                    self.close(cursor)
                    break

    @default_cursor([])
    def commit(self, cursor=None):
        commit_error = None
        with self.statistics as stt:
            for c in [cc for cc in self.cursors if cc.rowcount > 0]:
                stt.add_row_count(c.rowcount)
            if not self.auto_commit:
                try:
                    self.connection.commit()
                except DatabaseError as dbe:
                    commit_error = dbe
            self.close(cursor)
        if commit_error is not None:
            raise CommitException(str(commit_error))

    @default_cursor([])
    def rollback(self, cursor=None):
        if not self.auto_commit:
            self.connection.rollback()
        self.close(cursor)

    def query(self, sql: str, parameters=None, return_type=tuple, max_rows=0, array_size=1000):
        """
        Send a query SQL to the database and return rows of results
        @param sql: str - single sql statement
        @param parameters: list - list of parameters specified in the SQL (defaults to None)
        @param return_type: type of the rows return. May be list, tuple (default), dict, or OrderedDict
        @param max_rows: maximum number of rows to return. Zero or negative imply all
        @param array_size: batch size for which results are buffered when retrieving from the database
        @return: iterator of the specified return type, or the return type if max_rows=1
        """
        cur = self.execute(sql, parameters, cursor=None)
        if cur.rowcount >= 0:
            raise ValueError('The provided SQL is for updates, not to query. Use Execute method instead.')
        return self.get_data(cur, return_type=return_type, include_none=False, max_rows=max_rows, array_size=array_size)

    def query_single(self, sql: str, parameters=None, return_type=tuple) -> (tuple, list, dict, OrderedDict):
        result = None
        for r in self.query(sql, parameters=parameters, return_type=return_type, max_rows=1):
            result = r
            break
        return result

    def query_single_value(self, sql: str, parameters=None):
        result = self.query_single(sql, parameters, tuple)
        if len(result) > 0:
            return result[0]
        else:
            return None

    def get_int(self, sql: str, parameters=None):
        value = self.query_single_value(sql, parameters)
        if value is None:
            return 0
        elif isinstance(value, int):
            return value
        else:
            return int(value)

    def get_statistics(self, tag=None) -> str:
        """
        Return the query time of this instance as a string
        @return: query time of this instance as hh:mm:ss
        """
        if tag is None:
            tag = self.login
        return self.statistics.get_statistics(tag)
