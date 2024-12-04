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
        if self.verbose:
            ic(content)

    @staticmethod
    def create_connection_string(driver: str, server_ip: str, database: str, user_id: str, password: str, trusted: bool) -> str:
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