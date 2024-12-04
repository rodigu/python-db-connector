import pyodbc as db
from icecream import ic

TableColumns = set[str]

class DBConnector:
    def __init__(self, connection_string: str, verbose = False):
        """Creates connection to database

        Sample `connection_string`:

        ```
        Driver={SQL Server};
        Server=SERVER_IP;
        Database=ZENDESK;
        UID=USER_ID;
        PWD=PASSWORD;
        Trusted_Connection=no;
        ```

        :param str connection_string: connection string
        """
        self._connection_string = connection_string
        self._con = db.connect(connection_string)
        self.verbose = verbose
        self.table_columns: dict[str, TableColumns] = {}


    def get_table_columns(self, table: str) -> TableColumns:
        return { cn[0] for cn in self.execute(f"select column_name from information_schema.columns where TABLE_NAME='{table}'").fetchall() }

    def cache_table_columns(self, table: str):
        """Caches table columns from SQL database
        """
        self.table_columns['table'] = self.get_table_columns(table)

    def has_column(self, table: str, column: str) -> bool:
        return column in self.table_columns[table]

    def add_column(self, table: str, column: str, column_type: str):
        self.execute(f'alter table [{table}] add [{column}] {column_type} NULL')
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

    def has_table(self, table: str) -> bool:
        """Checks if given `table` exists in the connected database

        :param str table: table name
        :return bool: True if table exists in database, false otherwise
        """
        return bool(self._con.cursor().tables(table=table, tableType='TABLE').fetchone())

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
