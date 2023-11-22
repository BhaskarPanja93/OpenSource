"""
mysqlPool()
AutoScaling concurrent MySQL query handler that automatically creates and re-use old connections as per situation.


To USE:
Store the initialized class in a variable, Initialising the class needs:
    4 compulsory parameters
        user: Username for the database
        password: Password for the database
        dbName: Database name
    2 optional parameters
        host (default localhost): Location where the database is hosted e.g. localhost or any specific IP
        logFile (default None): Absolute or relative text file location to write all errors raised
        errorWriter (default None): CAN BE IGNORED. A custom function that takes 3 string arguments to write and/or process logs
"""


from time import sleep
import mysql.connector

class mysqlPool:
    def __init__(self, user, password, dbName, host="127.0.0.1", logFile=None, errorWriter=None):
        self.connections = []
        self.user = user
        self.host = host
        self.password = password
        self.dbName = dbName
        self.logFile = logFile
        self.errorWriter = errorWriter if errorWriter is not None else self.defaultErrorWriter


    def checkDatabaseStructure(self):
        """
        Override this function and implement code to check and create the database and the corresponding tables (if needed).

        Example code to create the database:
        if not self.run(f"SHOW DATABASES LIKE \"{self.db_name}\"", commit_required=False, database_required=False):
            self.execute(f"CREATE database {self.dbName};", database_required=False, commit_required=False)

        Example code to create a sample table:
        table_name = "song_data"
        if not self.run(f"SHOW TABLES LIKE \"{table_name}\"", commit_required=False):
            self.execute(f'''
                       CREATE TABLE IF NOT EXISTS `{self.db_name}`.`{table_name}` (
                       `_id` VARCHAR(100) NOT NULL,
                       `duration` INT ZEROFILL NULL,
                       `thumbnail` VARCHAR(100) NULL,
                       `audio_url` VARCHAR(2000) NULL,
                       `audio_url_created_at` TIMESTAMP NULL,
                       PRIMARY KEY (`_id`),
                       UNIQUE INDEX `_id_UNIQUE` (`_id` ASC) VISIBLE)
                       ''', commit_required=True))
        """

        pass


    def defaultErrorWriter(self, category:str, text:str, extras:str="", log:bool=True):
        """
        Demo(default) function to write MySQL errors to output and file
        :param category: Category of the error
        :param text: Main text of the error
        :param extras: Additional text
        :param log: Boolean specifying if the error has to be written to the file
        """
        string = f"[MYSQL POOL] [{category}]: {text} {extras}"
        print(string)
        if log:
            open(self.logFile, "a").write(string + "\n")


    def execute(self, syntax: str, commitRequired: bool, ignoreErrors: bool=True, dbRequired: bool=True)->None|list:
        """

        :param syntax: The MySQL syntax to execute
        :param commitRequired: Boolean specifying if a commit is required after the syntax is executed. Commit is required when a database-changing syntax is executed
        :param ignoreErrors: If errors are supposed to be caught promptly or sent to the main application
        :param dbRequired: Boolean specifying if the syntax is supposed to be executed on the database or not. A database creation syntax doesn't need the database to be already present, so the argument should be False for those cases
        :return: None or list of tuples depending on the syntax passed
        """
        destroyConnection = commitRequired
        while True:
            try:
                if not dbRequired:
                    connection = mysql.connector.connect(user=self.user, host=self.host, password=self.password)
                    destroyConnection = True
                elif self.connections:
                    connection = self.connections.pop()
                else:
                    connection = mysql.connector.connect(user=self.user, host=self.host, password=self.password, database=self.dbName)
                break
            except Exception as e:
                self.errorWriter("CONNECTION FAIL", repr(e))
                sleep(1)
        cursor = connection.cursor()
        data = None
        try:
            cursor.execute(syntax)
            if commitRequired:
                connection.commit()
            data = cursor.fetchall()
        except Exception as e:
            destroyConnection = True
            self.errorWriter("EXCEPTION", repr(e))
            if ignoreErrors:
                pass
            else:
                raise e
        if destroyConnection:
            connection.close()
        else:
            self.connections.append(connection)
        return data
