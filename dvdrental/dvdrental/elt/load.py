import logging 
from utility.upsert import generate_sqlalchemy_schema, upsert_in_chunks, upsert_all
from pandas import DataFrame
from sqlalchemy import MetaData


def upsert_to_database(df: DataFrame, table_name: str, key_columns: str, engine, chunksize:int=1000) -> bool: 

    """

    Upsert dataframe to a database table 


    Parameters
    -----------
    df: Dataframe 
        Source data

    table_name: str
        Name of the target table 

    key_columns: 
        Name of key columns to be used for upserting 

    engine: connection engine to database 
    
    chunksize: int 
        If chunksize greater than 0 is specified, then the rows will be inserted/upserted in
        the specified chunksize. e.g. 1000 rows at a time. 


    Return
    ------
    bool

    """
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")
    meta = MetaData()
    logging.info(f"Generating table schema: {table_name}")
    table_schema = generate_sqlalchemy_schema(df=df, key_columns=key_columns,table_name=table_name, meta=meta)
    meta.create_all(engine)
    logging.info(f"Table schema generated: {table_name}")
    logging.info(f"Writing to table: {table_name}")
    if chunksize > 0:
        upsert_in_chunks(df=df, engine=engine, table_schema=table_schema, key_columns=key_columns, chunksize=chunksize)
    else: 
        upsert_all(df=df, engine=engine, table_schema=table_schema, key_columns=key_columns)
    logging.info(f"Successful write to table: {table_name}")
    return True 






def overwrite_to_database(df: DataFrame, table_name: str, engine) -> bool: 
    """
    Upsert dataframe to a database table. Overwrites the table


    Parameters
    ----------
    df: Dataframe 
        Source data frame 

    table_name: str 
        name of the target table 

    engine: PostgreSQL engine
        connection engine to database 

    
    Returns
    -------
    bool

    """

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")
    logging.info(f"Writing to table: {table_name}")
    df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
    logging.info(f"Successful write to table: {table_name}, rows inserted/updated: {len(df)}")
    return True 
