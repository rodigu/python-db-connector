import pandas as pd
import pyodbc as db
from icecream import ic
from dataclasses import dataclass


TableColumns = set[str]

@dataclass
class TypedColumn:
    column: str
    value: any
    type: str

# Dictionary with column name-typed column pairs
ColumnTypeList = dict[str, TypedColumn]

@dataclass
class TypeMapper:
    """TypeMapper class

    Contains mapping from python/pandas types to SQL types.

    Priority chain: `direct -> prefix -> suffix -> typed`

    Will use first dictionary where it finds the given value

    :param dict[str, str] direct: direct mapping, will map from column name (dict key) to SQL type (dict value)
    :param dict[str, str] prefix: will map from column name that has given prefix (dict key) to SQL type (dict value)
    :param dict[str, str] suffix: will map from column name that has given suffix (dict key) to SQL type (dict value)
    :param dict[str, str] typed: will map from pandas type (dict key) to SQL type (dict value)
    """
    direct: dict[str, str]
    prefix: dict[str, str]
    suffix: dict[str, str]
    typed: dict[str, str]

    def map(self, column_name: str, column_type: str) -> str:
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
    def __init__(self, connection_string: str, table: str, type_mapper: TypeMapper, verbose=False, id_column='id'):
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
        """
        self.id_column = 'id'
        self.type_mapper = type_mapper
        self.table = table
        self._connection_string = connection_string
        self._con = db.connect(connection_string)
        self.verbose = verbose
        self.table_columns: dict[str, TableColumns] = {}
        self._crsr = self._con.cursor()

    def get_table_columns(self) -> TableColumns:
        return { cn[0] for cn in self.execute(f"select column_name from information_schema.columns where TABLE_NAME='{self.table}'").fetchall() }

    def cache_table_columns(self):
        """Caches table columns from SQL database
        """
        self.table_columns['table'] = self.get_table_columns(self.table)

    def has_column(self, column: str) -> bool:
        return column in self.table_columns[self.table]

    def add_column(self, column: str, column_type: str):
        self.execute(f'alter table [{self.table}] add [{column}] {column_type} NULL')
        self.commit()

    def vp(self, content: str):
        """Verbose prints.

        Passes `content` to icecream's `ic` if `self.verbose` is set to `True`

        :param str content: string to be sent to `ic`
        """
        if self.verbose:
            ic(content)

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
        """
        return f"Driver={driver};Server={server_ip};Database={database};UID={user_id};PWD={password};Trusted_Connection={'yes' if trusted else 'no'};"

    def has_table(self) -> bool:
        """Checks if given `table` exists in the connected database

        :param str table: table name
        :return bool: True if table exists in database, false otherwise
        """
        return bool(self._con.cursor().tables(table=self.table, tableType='TABLE').fetchone())

    def reconnect(self):
        self._con = db.connect(self._connection_string)

    def commit(self, reconnect_attempts=1):
        """Commits executed queries to database.

        :param int reconnect_attempts: number of times to retry connecting to the database if `commit` throws an error, defaults to 1
        """
        try:
            self._con.commit()
        except db.Error as pe:
            self.vp(f"Error: {pe}")
            if reconnect_attempts == 0:
                return
            self.vp(f'Retrying commit ({reconnect_attempts} attempts left)')
            try:
                self.reconnect()
                self.commit(reconnect_attempts - 1)
            except:
                self.vp("Couldn't reconnect")

    def execute(self, sql_query: str, tries=10, is_first=True):
        """Executes the given SQL query.

        :param str sql_query: SQL query string
        :param int tries: number of times to try executing the query before giving up, defaults to 10
        :param bool is_first: used to track number of tries, defaults to True
        """
        if tries==0:
            self.vp(f"Couldn't execute query: {sql_query}")
            return

        try:
            r = self._con.execute(sql_query)
            if not is_first:
                self.vp("Execute successful")
            return r
        except db.Error as pe:
            self.vp(f"Error: {pe}")
            try:
                self.vp(f'Retrying execute ({tries} attempts left)')
                return self.execute(sql_query, tries - 1, False)
            except:
                self.vp(f"Couldn't execute query: {sql_query}")
                return

    def typed_columns(self, obj_dict: dict) -> ColumnTypeList:
        """List with column name, value and types extracted from given object dictionary.

        :param dict obj_dict: dictionary
        :return list[dict[str, any]]: list of dictionaries
        """
        df = pd.json_normalize(obj_dict)
        pd_types = df.dtypes.to_dict()

        return [TypedColumn(column=key, value=val, type=self.type_mapper.map(key, str(pd_types[key]))) for key, val in df.iloc[0].to_dict().items() if val is not None]

    @staticmethod
    def flatten_dict(d: dict, key: str|list[str] = []) -> dict:
        """Recusively flattens a dict.
        Lists with dictionaries are turned into dicts with keys using `key`.

        If `key` is a list, it attempts to use each item from the list in order as a key.

        :param dict d: dictionary to be flattened
        :param str | list[str] key: key or keys to be used in keying lists of dictionaries
        :return dict: flatenned dictionary

        >>> DBConnector.flatten_dict({'a': 1, 'b': 2})
        >>> DBConnector.flatten_dict({'a': 1, 'b': [{'k': 2}]})
        >>> DBConnector.flatten_dict({'a': 1, 'b': [{'k': 2}, {'k': 1}]}, key='k')
        """
        return { k: DBConnector.flatten_dict_list(v, key) for k, v in d.items() }

    @staticmethod
    def flatten_dict_list(dlist: list[dict], key: str|list[str]) -> dict:
        """Flattens list of dictionaries recursively

        Expects list of dicts to be uniform in terms of keys.

        If no keys are found in the first dictionary of the list, it returns the list in string form.

        :param list[dict] dlist: list of dictionaries
        :param str | list[str] key: key(s) to be extrated from the dictionaries and turned into dict keys
        :return dict: flattened dictionary
        """
        wrong_type = lambda x: type(x)!=list and type(x)!=dict
        if wrong_type(dlist) or (type(dlist)==list and len(dlist) > 0 and wrong_type(dlist[0])):
            return dlist

        if type(dlist)==dict:
            return DBConnector.flatten_dict(dlist, key)

        no_keys_in_dict = (choose_key(dlist[0]) not in dlist[0] and choose_key(dlist[0]) == None)

        if no_keys_in_dict:
            return str(dlist)

        def choose_key(d: dict) -> str:
            if type(key)==str: return key
            for k in key:
                if k in d: return k
            return None
        flattened = { d[choose_key(d)]: DBConnector.flatten_dict_list(d, key) for d in dlist }
        for v in flattened.values():
            v.pop(choose_key(v))
        return flattened

    def add_columns(self, typed_columns: ColumnTypeList):
        """Adds columns in `ColumnTypeList` to working table

        :param ColumnTypeList typed_columns: dictionary of column names and their respective types
        """
        for column, t in typed_columns.items():
            if not self.has_column(column):
                self.vp(f'Column {column} ({t.type}) does not exist, adding it now.')
                self.add_column(column, t.type)
                self.cache_table_columns()

    @staticmethod
    def parse_value(data: TypedColumn) -> tuple[str, str]:
        """Converts python values into valid SQL values.

        :param TypedColumn data: original python data
        :return tuple[str, str]: tuple of column name and SQL-type
        """

        if (data.value is None) or (type(data.value)==str and len(data.value)==0):
            return (data.column, 'NULL')
        if data.type == 'bit':
            return (data.column, str(int(data.value)))
        if 'int' in data.type:
            return (data.column, f'{data.value}')
        return (data.column, f"N'{str(data.value).replace("'", '"')}'")

    def sql_update_str(self, typed_columns: ColumnTypeList, id: str) -> str:
        """Updates row with `id` using the given `typed_columns`

        :param ColumnTypeList typed_columns: 
        :param str id: row ID
        :return str: SQL query string
        """
        parsed_values: dict[str, str] = dict(map(DBConnector.parse_value, typed_columns.values()))
        return f"update {self.table} set {', '.join([f'[{c}]={v}' for c, v in parsed_values.items()])} where {self.id_column}={id if type(id)==int else f"'{id}'"}"

    def sql_columns_and_values(self, typed_columns: ColumnTypeList) -> tuple[str, str]:
        """Generates SQL strings for columns and values from a given `ColumnTypeList`

        :param ColumnTypeList typed_columns:
        :return tuple[str, str]: tuple with columns and values strings (respectively)
        """
        return f"[{'], ['.join([t.column for t in typed_columns])}]", ', '.join(dict(map(DBConnector.parse_value, typed_columns.values())).values())

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
        :yield list[any]: list of values for the current row
        """
        self._crsr.execute(selection_str)
        while (row:=self._crsr.fetchone()) is not None:
            yield row
