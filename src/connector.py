import pyodbc as db
from icecream import ic

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
        self._con = db.connect(connection_string)
        self.verbose = verbose

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
        return bool(self._con.cursor().tables(table=table, tableType='TABLE').fetchone())

    def execute(self, sql_query: str, tries=10, is_first=True):
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