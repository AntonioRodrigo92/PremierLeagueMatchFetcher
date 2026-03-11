# pip install --only-binary :all: sqlalchemy greenlet pymysql

from sqlalchemy import create_engine, exc


class WriteToMariaDB:
    def __init__(self, username, password, host, port, database):
        self.db_url = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(self.db_url)

    def write_dataframe(self, df, table_name, if_exists='replace', index=False):
        try:
            with self.engine.begin() as connection:
                df.to_sql(table_name, con=connection, if_exists=if_exists, index=index)
            print(f"Successfully wrote {len(df)} rows to table '{table_name}'.")
        except exc.SQLAlchemyError as e:
            print(f"An error occurred: {e}")