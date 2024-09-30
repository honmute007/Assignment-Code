from sqlalchemy import Table, Column, MetaData, Float
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
import csv
import pandas as pd
import math
from CustomError import EmptyTable


class BaseTable:
    """
    Importing the CSV file to SqLite Database
    :param column_name: Names of column
    :param table_name: Heading of the table
    :return: NONE
    """

    def __init__(self, column_name, table_name):
        self.column_name = column_name
        self.table_name = table_name

    def import_csv_to_database(self, engine, file_name):
        """
        :param engine: SQLite database
        :param file_name: intended name of the table
        :return:
        """
        metadata = MetaData()
        try:
            with engine.connect() as connection:
                quoted_table_name = connection.dialect.identifier_preparer.quote(self.table_name)
                query = connection.execute(
                    text(f"SELECT CASE WHEN COUNT(*) > 0 THEN 'Table is not empty' ELSE 'Table is "
                         f"empty' END FROM {quoted_table_name}"))
                result = query.fetchone()
            if result[0] == "Table is not empty":
                return result[0]
            else:
                raise EmptyTable
        except (Exception, EmptyTable):
            print("Table is not empty")

        try:
            csv_table = Table(self.table_name, metadata,
                              *[Column(col_name, Float, nullable=True) for col_name in self.column_name])
            metadata.create_all(engine)
            Session = sessionmaker(bind=engine)
            session = Session()
            with open(file_name, 'r') as file:
                reader = csv.DictReader(file)
                data_to_insert = [{key: row[key] for key in row} for row in reader]
                j = [key for key in data_to_insert[0].keys()]
                res = {self.column_name[i]: j[i] for i in range(len(self.column_name))}
                for a in data_to_insert:
                    for b, c in res.items():
                        a[b] = a.pop(c)
                session.execute(csv_table.insert(), data_to_insert)
                session.commit()
            session.close()
            print(f" Data is added added to {self.table_name}")
        except:
            print(f"Data not added to the SQLite {self.table_name}")

    def get_table_as_dataframe(self, engine):
        """
        param engine: SQLite database
        :return - data fletched from the SQL data base in Pandas dataframe object
        """
        with engine.connect() as connection:
            quoted_table_name = connection.dialect.identifier_preparer.quote(self.table_name)
            query = connection.execute(text(f"SELECT * FROM {quoted_table_name}"))
            df = pd.DataFrame(query.fetchall(), columns=query.keys())
        return df


# Class for specific tables
class TrainingDataTable(BaseTable):
    """
    inherit the data from BaseTable class
    """

    def __init__(self, column_name, table_name):
        super().__init__(column_name, table_name)


class IdealFunctionsTable(BaseTable):
    """
    inherit the data from BaseTable class
    """

    def __init__(self, column_name, table_name):
        super().__init__(column_name, table_name)


class TestDataTable(BaseTable):
    """
    inherit the data from BaseTable class
    """

    def __init__(self, column_name, table_name):
        super().__init__(column_name, table_name)


class TestDataTableWithDeviation(BaseTable):
    """
    inherit the data from BaseTable class
    """

    def __init__(self, df_data, column_name, table_name):
        super().__init__(column_name, table_name)
        self.df_data = df_data

    def df_to_sqlite(self, engine):
        """
        Pushes the Pandas dataframe to SQlite database
        param engine: SQLite database
        :rtype: object
        """
        try:
            with engine.connect() as connection:
                self.df_data.to_sql(self.table_name, connection, if_exists='append', index=False)
                connection.close()
        except:
            print("data is not added to Sqlite database")
