from sqlalchemy import create_engine
import pandas as pd
import logging

# take a DataFrame and load it into a SQL database
# 1. Connects to a database using SQLAlchemy
# 2.Writes your reconciled DataFrame into a table
# 3.Logs success or failure
# 4.Raises the error if something goes wrong
def load_to_database(df: pd.DataFrame, db_url: str, table_name: str):
    # df → the DataFrame you want to load
    # db_url → connection string (e.g., SQLite, Postgres, SQL Server)
    # table_name → the name of the table to create/replace
    """Loads reconciled dataframe into the reporting database."""
    try:
    # “Try to run this code. If anything fails, jump to the except block.”
        engine = create_engine(db_url)
        # SQLAlchemy’s create_engine() creates a connection object to your database.
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        # Write the DataFrame to the database
        logging.info(f"Successfully loaded {len(df)} records into {table_name} table.")
    except Exception as e:
        # This catches any error that happens inside the try block.
        # Exception = the base class for most Python errors, so it will catch anything unexpected.
        #  We store the error message in variable e.  
        logging.error(f"Failed to load data to database: {e}")
        raise
        # raise means: stop the current function immediately and send the error upward so the caller knows something went wrong.