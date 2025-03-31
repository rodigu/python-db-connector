import pandas as pd
import pyodbc as db
from icecream import ic
from dataclasses import dataclass, field


TableColumns = set[str]

@dataclass
class TypedColumn:
    column: str
    type: str
    value: any = None

# Dictionary with column name-typed column pairs
ColumnTypeList = list[TypedColumn]

@dataclass
class TypeMapper:
    """TypeMapper class

    Contains mapping from python/pandas types to SQL types.

    Priority chain: `direct -> prefix -> suffix -> typed`

    Will use first dictionary where it finds the given value

    :param dict[str, str] direct: direct mapping, will map from column name (dict key) to SQL type (dict value)
    :param dict[str, str] prefix: will map from column name that has given prefix (dict key) to SQL type (dict value), defaults to an empty dictionary
    :param dict[str, str] suffix: will map from column name that has given suffix (dict key) to SQL type (dict value), defaults to an empty dictionary
    :param dict[str, str] typed: will map from pandas type (dict key) to SQL type (dict value), defaults to an empty dictionary

    >>> tm = TypeMapper(
    ...     direct={'sample_column': 'int', 'another_column': 'varchar(10)'},
    ...     prefix={'pre_': 'varchar(10)'},
    ...     suffix={'_su': 'int'},
    ...     typed={'int64': 'int', 'float64': 'decimal', 'bool': 'bit', 'object': 'varchar(max)'}
    ... )
    >>> tm.map(column_name='sample_column')
    'int'
    >>> tm.map(column_name='another_column')
    'varchar(10)'
    >>> tm.map(column_name='pre_column')
    'varchar(10)'
    >>> tm.map(column_name='column_su')
    'int'
    >>> tm.map(column_name='any_name', column_type='bool')
    'bit'
    """
    direct: dict[str, str] = field(default_factory=dict)
    prefix: dict[str, str] = field(default_factory=dict)
    suffix: dict[str, str] = field(default_factory=dict)
    typed: dict[str, str] = field(default_factory=dict)

    def map(self, column_name: str, column_type: str=None) -> str:
        if column_name in self.direct:
            return self.direct[column_name]
        for p in self.prefix:
            if column_name[:len(p)] == p:
                return self.prefix[p]
        for s in self.suffix:
            if column_name[-len(s):] == s:
                return self.suffix[s]
        if column_type in self.typed:
            return self.typed[column_type]

class DBConnector:
    def __init__(self, connection_string: str, table: str, type_mapper: TypeMapper|dict=TypeMapper(), verbose=False, id_column='id', logger=ic, composite_kwargs: dict=None, do_fast_executemany=False):
        """Creates connection to database

        Sample `connection_string`:

        ```
        Driver={SQL Server};
        Server=SERVER_IP;
        Database=DB_NAME;
        UID=USER_ID;
        PWD=PASSWORD;
        Trusted_Connection=no;
        ```

        :param str connection_string: connection string
        :param str table: working table name
        :param bool verbose: whether to verbose print, defaults to True
        :param str id_column: column to be used as ID for update functions
        :param Callable logger: logging function, defaults to `ic`
        """
        if type(type_mapper) == dict:
            type_mapper = TypeMapper(**type_mapper)
        self.id_column = id_column
        self.type_mapper = type_mapper
        self.table = table
        self._connection_string = connection_string
        self._con = db.connect(connection_string)
        self.verbose = verbose
        self.cache_table_columns()
        self._crsr = self._con.cursor()
        self.logger = logger
        self.id_cache = None
        self.df: pd.DataFrame = None
        self.do_composite_id = composite_kwargs is not None
        self.composite_kwargs: dict = composite_kwargs
        if self.do_composite_id:
            self.id_column = self.composite_kwargs['id_name']
        self.do_fast_executemany = do_fast_executemany

    def get_table_columns(self) -> TableColumns:
        return { cn[0] for cn in self.execute(f"select column_name from information_schema.columns where TABLE_NAME='{self.table}'").fetchall() }

    def cache_table_columns(self):
        """Caches table columns from SQL database
        """
        self.table_columns = self.get_table_columns()

    def has_column(self, column: str) -> bool:
        return column in self.table_columns

    def add_column(self, column: str, column_type: str):
        self.execute(f'alter table [{self.table}] add [{column}] {column_type} NULL')
        self.commit()

    def vp(self, content: str):
        """Verbose prints.

        Passes `content` to `logger` (`ic`, if unset) if `self.verbose` is set to `True`]

        :param str content: string to be sent to `logger`
        """
        if self.verbose:
            self.logger(content)

    @staticmethod
    def create_connection_string(driver: str, server_ip: str, database: str, user_id: str, password: str, trusted=False) -> str:
        """Creates connection string using the given auth values, see the [`pyodbc`](https://github.com/mkleehammer/pyodbc/wiki/Getting-started) docs for more info.

        :param str driver: driver name, e.g. {SQL Server}
        :param str server_ip: server ip number string (should incluede periods)
        :param str database: database name to connect to
        :param str user_id: user id
        :param str password: user password
        :param bool trusted: if the connection is trusted, defaults to False
        :return str: pyodbc-valid connection string

        >>> DBConnector.create_connection_string('{DRIVER_NAME}', 'SERVER_IP', 'DATABASE', 'USER_ID', 'USER_PASSWORD')
        'Driver={DRIVER_NAME};Server=SERVER_IP;Database=DATABASE;UID=USER_ID;PWD=USER_PASSWORD;Trusted_Connection=no;'
        """
        return f"Driver={driver};Server={server_ip};Database={database};UID={user_id};PWD={password};Trusted_Connection={'yes' if trusted else 'no'};"

    def has_table(self) -> bool:
        """Checks if given `table` exists in the connected database

        :param str table: table name
        :return bool: True if table exists in database, false otherwise
        """
        return bool(self._con.cursor().tables(table=self.table, tableType='TABLE').fetchone())

    def create_table(self, type_list: ColumnTypeList):
        self.execute(sql_query=f'create table {self.table}({', '.join((f'[{t.column}] {t.type}' for t in type_list))})')
        self.cache_table_columns()
        self.commit()

    def reconnect(self):
        self._con = db.connect(self._connection_string)

    def commit(self, reconnect_attempts=1):
        """Commits executed queries to database.

        :param int reconnect_attempts: number of times to retry connecting to the database if `commit` throws an error, defaults to 1
        """
        try:
            self._con.commit()
        except db.Error as pe:
            self.vp(f"\t---commit error: {pe}")
            if reconnect_attempts == 0:
                return
            self.vp(f"\t---reattempting commit ({reconnect_attempts})")
            try:
                self.reconnect()
                self.commit(reconnect_attempts - 1)
            except:
                self.vp(f"\t---couldn't reconnect to db")

    def execute(self, sql_query: str, tries=10, is_first=True, current=1):
        """Executes the given SQL query.

        :param str sql_query: SQL query string
        :param int tries: number of times to try executing the query before giving up, defaults to 10
        :param bool is_first: used to track number of tries, defaults to True
        """
        _tabs = '\t' * current

        if tries==0:
            self.logger(f"{_tabs}---failed to execute {sql_query}")
            return

        try:
            r = self._con.execute(sql_query)
            if not is_first:
                self.vp(f"{_tabs}---execute successful")
            return r
        except db.Error as pe:
            self.vp(f"{_tabs}---could not execute: {pe}")
            try:
                self.vp(f"{_tabs}---attempt {current} ({tries} left)")
                return self.execute(sql_query, tries - 1, False, current + 1)
            except:
                return

    def typed_columns(self, obj_dict: dict, do_keep_nulls=False) -> ColumnTypeList:
        """List with column name, value and types extracted from given object dictionary.

        :param dict obj_dict: dictionary
        :return list[dict[str, any]]: list of dictionaries
        """
        df = pd.json_normalize(obj_dict)
        pd_types = df.dtypes.to_dict()

        skip_none = lambda v: True if do_keep_nulls else (v is not None)

        return [ TypedColumn(column=key, value=val, type=self.type_mapper.map(key, str(pd_types[key]))) for key, val in df.iloc[0].to_dict().items() if skip_none(val) ]

    @staticmethod
    def flatten_dict(d: dict, key: str|list[str] = []) -> dict:
        """Recusively flattens a dict.
        Lists with dictionaries are turned into dicts with keys using `key`.

        If `key` is a list, it attempts to use each item from the list in order as a key.

        :param dict d: dictionary to be flattened
        :param str | list[str] key: key or keys to be used in keying lists of dictionaries
        :return dict: flatenned dictionary

        >>> DBConnector.flatten_dict({'a': 1, 'b': 2})
        {'a': 1, 'b': 2}
        >>> DBConnector.flatten_dict({'a': 1, 'b': [{'k': 2}]})
        {'a': 1, 'b': "[{'k': 2}]"}
        >>> DBConnector.flatten_dict({'a': 1, 'b': [{'k': 2, 'val': 0}, {'k': 1, 'val': 2}]}, key='k')
        {'a': 1, 'b': {2: {'val': 0}, 1: {'val': 2}}}
        """
        return { k: DBConnector.flatten_dict_list(v, key) for k, v in d.items() }

    @staticmethod
    def flatten_dict_list(dlist: list[dict], key: str|list[str]):
        """Flattens list of dictionaries recursively

        Expects list of dicts to be uniform in terms of keys.

        If no keys are found in the first dictionary of the list, it returns the list in string form.

        :param list[dict] dlist: list of dictionaries
        :param str | list[str] key: key(s) to be extrated from the dictionaries and turned into dict keys
        :return: flattened dictionary or value

        >>> DBConnector.flatten_dict_list([{'k': 2, 'val': 0}, {'k': 1, 'val': 2}], key='k')
        {2: {'val': 0}, 1: {'val': 2}}
        """
        wrong_type = lambda x: type(x)!=list and type(x)!=dict
        if wrong_type(dlist) or (type(dlist)==list and len(dlist) > 0 and wrong_type(dlist[0])):
            return dlist

        if type(dlist)==dict:
            return DBConnector.flatten_dict(dlist, key)

        def choose_key(d: dict) -> str:
            if type(key)==str: return key
            for k in key:
                if k in d: return k
            return None

        no_keys_in_dict = (choose_key(dlist[0]) not in dlist[0] and choose_key(dlist[0]) == None)

        if no_keys_in_dict:
            return str(dlist)

        flattened = { d[choose_key(d)]: DBConnector.flatten_dict_list(d, key) for d in dlist }
        for v in flattened.values():
            v.pop(choose_key(v))
        return flattened

    def add_columns(self, typed_columns: ColumnTypeList):
        """Adds columns in `ColumnTypeList` to working table

        :param ColumnTypeList typed_columns: dictionary of column names and their respective types
        """
        for t in typed_columns:
            if not self.has_column(t.column):
                self.add_column(t.column, t.type)
                self.cache_table_columns()

    @staticmethod
    def parse_value(data: TypedColumn) -> tuple[str, str]:
        """Converts python values into valid SQL values.

        :param TypedColumn data: original python data
        :return tuple[str, str]: tuple of column name and SQL-type

        >>> DBConnector.parse_value(TypedColumn('none_column', None, any))
        ('none_column', 'NULL')
        >>> DBConnector.parse_value(TypedColumn('empty_string', '', str))
        ('empty_string', 'NULL')
        >>> DBConnector.parse_value(TypedColumn('bool_column', True, 'bit'))
        ('bool_column', '1')
        >>> DBConnector.parse_value(TypedColumn('int_column', 10, 'int'))
        ('int_column', '10')
        >>> DBConnector.parse_value(TypedColumn('other_column', "text string", 'varchar(max)'))
        ('other_column', "N'text string'")

        """

        if data.type == 'datetime':
            return (data.column, data.value)
        if (data.value is None) or (type(data.value)==str and len(data.value)==0):
            return (data.column, 'NULL')
        if data.type == 'bit':
            return (data.column, str(int(data.value)))
        if 'int' in data.type:
            return (data.column, int(data.value))
        return (data.column, data.value)

    def sql_update_str(self, typed_columns: ColumnTypeList, id: str) -> str:
        """Updates row with `id` using the given `typed_columns`

        :param ColumnTypeList typed_columns: 
        :param str id: row ID
        :return str: SQL query string
        """
        parsed_values: dict[str, str] = dict(map(DBConnector.parse_value, typed_columns))
        return f"update {self.table} set {', '.join([f'[{c}]={v}' for c, v in parsed_values.items()])} where {self.id_column}={id if type(id)==int else f"'{id}'"}"

    def sql_columns_and_values(self, typed_columns: ColumnTypeList) -> tuple[str, str]:
        """Generates SQL strings for columns and values from a given `ColumnTypeList`

        :param ColumnTypeList typed_columns:
        :return tuple[str, str]: tuple with columns and values strings (respectively)
        """
        return f"[{'], ['.join([t.column for t in typed_columns])}]", ', '.join(dict(map(DBConnector.parse_value, typed_columns)).values())

    def sql_insertion_str(self, typed_columns: ColumnTypeList) -> str:
        """Creates valid SQL string to insert a row using the given typed columns.

        :param ColumnTypeList typed_columns:
        :return str: SQL insert string
        """
        columns, values = self.sql_columns_and_values(typed_columns)
        return f'insert into {self.table} ({columns}) values ({values})'

    def select(self, selection_str: str):
        """Yields row-by-row results for the given SQL selection string

        :param str selection_str:
        :yield tuple[any]: list of values for the current row
        """
        self._crsr.execute(selection_str)
        while (row:=self._crsr.fetchone()) is not None:
            yield row

    @staticmethod
    def connection_from_file(json_fname: str, table: str, **args) -> 'DBConnector':
        """
        JSON file should have keys such that it fits into the function signature of `DBConnector.create_connection_string`:

        ```json
        {
            "driver": "DRIVER",
            "server_ip": "SERVER IP",
            "database": "DATABASE",
            "user_id": "USER ID",
            "password": "USER PASSWORD",
            "trusted": 0
        }
        ```

        Other keyword arguments will e passed onto the DBConnector constructor.

        :param str json_fname: name of the JSON file with the database connection information and auth
        :return DBConnector: DBConnector instance created using information from given JSON file
        """
        import json
        with open(json_fname) as f:
            data = json.load(f)
        connection_string = DBConnector.create_connection_string(**data)
        return DBConnector(connection_string, table, **args)

    def get_table_ids(self, recache=True) -> set[int]:
        """Retrieves ID column from given table.

        :param bool recache: if True, table IDs weill be recached through a call to the SQL table.
        :return set: set with IDs
        """
        if recache or self.id_cache is None:
            ids = self.execute(f'select [{self.id_column}] from [{self.table}]')
            self.id_cache = set(i[0] for i in ([] if ids is None else ids.fetchall()))
        return self.id_cache

    def insert_dict(self, obj_dict: dict, recache=True, force=False, do_create_columns=True, do_composite_id=False, composite_id_kwargs={}) -> bool:
        """Inserts generic dictionary to table

        :param dict obj_dict: dictionary to be appended
        :param bool recache: whether to recache table IDs, defaults to True
        :param bool force: if object ID is already present in the table, the row will be updated with the given values inside `obj_dict`, defaults to False
        :param bool do_create_columns: if columns don't exist in the table, they will be added to the table (as opposed to throwing an error when set to False), defaults to True
        :param bool do_composite_id: if True, adds a composite ID to the flattened dictionary using the `DBConnector.composite_id_type_column` function, defaults to False
        :param bool composite_id_kwargs: if `do_composite_id` is set to True, this parameter will be destructured and fed into `DBConnector.composite_id_type_column`, defaults to {}
        :return bool: if append was successful
        """
        type_list = self.typed_columns(obj_dict)

        if do_composite_id:
            self.id_column = composite_id_kwargs['id_name']
            type_list += [ DBConnector.composite_id_type_column(type_list, ** composite_id_kwargs) ]
        id = [ t for t in type_list if t.column==self.id_column ][0].value

        if not self.has_table():
            self.create_table(type_list)

        if id in self.get_table_ids(recache):
            if not force:
                return False

        self.vp(f'> {id}')

        if do_create_columns:
            self.add_columns(type_list)

        sql_query = self.sql_insertion_str(type_list)

        if id in self.get_table_ids(recache):
            self.vp(f'\t...updating')
            sql_query = self.sql_update_str(type_list, id)

        self.id_cache.add(id)

        self.execute(sql_query=sql_query)
        self.commit()
        return True

    @staticmethod
    def composite_id_dict(original_dict: dict, id_name: str, id_keys: list, separator='+') -> dict:
        """Takes an `original_dict` and returns a dictionary with the same keys, and an additional key-value pair.

        The aditional key-value pair has a `key=id_name`
        and a value equals to a concatenation of the values in each respective key from the `original_dict`

        :param dict original_dict: original dictionary
        :param str id_name: name of the new ID key
        :param list id_keys: keys from `original_dict` that will be concatenated into `id_name`
        :param str separator: separator used in concatenation of values, defaults to '+'
        :return dict: new dictionary (shallow copy of `original_dict`)
        """
        return dict(**{ id_name: separator.join((str(original_dict[k]) for k in id_keys)) }, **original_dict)

    @staticmethod
    def composite_id_type_column(type_list: ColumnTypeList, id_name: str, id_keys: set, separator='+') -> TypedColumn:
        return TypedColumn(
            column=id_name,
            value=separator.join(str(x.value) for x in type_list if x.column in id_keys),
            type="varchar(max)"
        )

    def append_to_batch(self, dictionary: dict):
        """Appends given dictionary to batch dataframe in cache

        Does not execute to SQL databse.

        :param dict dictionary: dictionary of values
        """
        if self.df is None:
            self.df = pd.json_normalize(dictionary)
            # create table if it doesn't exist
            if not self.has_table():
                # convert first row into type_list
                pd_types = self.df.dtypes.to_dict()
                type_list = [ TypedColumn(column=key, type=self.type_mapper.map(key, str(pd_types[key]))) for key, _ in self.df.iloc[0].to_dict().items() ]
                self.create_table(type_list)
            return
        self.df = pd.concat([ self.df, pd.json_normalize(dictionary) ], ignore_index=True)

    @staticmethod
    def concatenated_id_column(df: pd.DataFrame, id_keys: list[str], separator='+') -> pd.Series:
        return df[id_keys[0]].str.cat(df[id_keys[1:]].astype(str), sep=separator)

    def executemany(self, iterable_values, query_string: str, tries=10, current=0, is_first=True):
        _tabs = ' ' * current

        if tries==0:
            self.logger(f"{_tabs}---failed to execute {query_string} with {iterable_values}")
            return

        try:
            cursor = self._con.cursor()
            cursor.fast_executemany = self.do_fast_executemany
            r = cursor.executemany(query_string, iterable_values)

            cursor.commit()

            if not is_first:
                self.vp(f"{_tabs}---execute successful")
            return r
        except db.Error as pe:
            self.vp(f"{_tabs}---could not execute: {pe}")
            try:
                self.vp(f"{_tabs}---attempt {current} ({tries} left)")
                return self.executemany(iterable_values, query_string=query_string, tries=tries - 1, is_first=False, current=current + 1)
            except:
                return

    def execute_batch(self):
        """Executes batch cached in dataframe, then clears cache
        """

        if self.do_composite_id:
            self.df[self.composite_kwargs['id_name']] = DBConnector.concatenated_id_column(self.df, id_keys=self.composite_kwargs['id_keys'])

        # convert first row into type_list
        pd_types = self.df.dtypes.to_dict()
        type_list = [ TypedColumn(column=key, type=self.type_mapper.map(key, str(pd_types[key]))) for key, _ in self.df.iloc[0].to_dict().items() ]

        for typed_col in type_list:
            if typed_col.type=='datetime':
                self.df[typed_col.column] = pd.to_datetime(db.df[typed_col.column]).dt.strftime('%Y-%m-%d %H:%M:%S')

        # update dicts that are already in cache
        update_df = self.df[self.df[self.id_column].isin(self.get_table_ids(recache=False))]
        # append dicts that aren't
        insert_df = self.df[~self.df[self.id_column].isin(self.get_table_ids(recache=False))]

        # create columns that don't exist
        self.add_columns(type_list)

        number_of_columns = len(self.df.columns)

        columns = f"[{'],['.join(self.df.columns)}]"
        question_marks = ('?,' * number_of_columns)[:-1]

        if len(insert_df) > 0:
            insertion_query = f"insert into {self.table} ({columns}) values ({question_marks})"
            self.executemany(
                tuple(tuple(row.values) for _, row in insert_df.iterrows()),
                query_string=insertion_query
            )
        if len(update_df) > 0:
            update_query = f"update {self.table} set {', '.join([f'[{c}]=?' for c in self.df.columns])} where [{self.id_column}]=?"
            self.executemany(
                tuple(tuple(row.values) + (row[self.id_column],) for _, row in update_df.iterrows()),
                query_string=update_query
            )

        # add newly appended dicts to cache
        self.id_cache |= set(self.df[self.id_column])

        self.df = None


if __name__ == "__main__":
    import doctest
    doctest.testmod()
