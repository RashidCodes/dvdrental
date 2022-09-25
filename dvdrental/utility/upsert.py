import os 
import logging 
import datetime as dt 
import jinja2 as j2
from numpy import nan
from pandas import read_csv, DataFrame, concat
from sqlalchemy import MetaData, Column, Integer, String
from sqlalchemy import Table, DateTime, Boolean, Numeric
from sqlalchemy.dialects import postgresql



def is_incremental(table: str, path: str) -> bool:

    """ 
    
    Check if an extract query is incremental or not 

    Parameters
    ----------
    table: str 
        The name of the extract query (without .sql extension)

    path: str 
        The location of the extract query 


    Returns
    -------
    bool
    
    
    """

    with open(f"{path}/{table}.sql") as f:
        raw_sql = f.read()

    try:
        config = j2.Template(raw_sql).make_module().config 
        return config.get("extract_type").lower() == "incremental"

    except:
        return False 




def get_key_columns(table: str, path: str = "extract_queries") -> list:

    """

    Get the list of primary key columns from the .sql file. Key columns are expxressed
    as `{% set key_columns = ["keyA", "keyB"] %}` in the .sql file 


    Parameters
    ----------
    table: str 
        The name of the extract query without the .sql extension


    path: str  
        The location of the extract query 


    Returns
    -------
    key_columns: list 
        A list of key columns in the table 


    """

    with open(f"{path}/{table}.sql") as f:
        raw_sql = f.read()

    try:
        key_columns = j2.Template(raw_sql).make_module().config["key_columns"]
        return key_columns
    except:
        return []




def get_sqlalchemy_column(column_name: str, source_datatype: str, primary_key:bool = False) -> Column:

    """

    Map pandas dtypes to SQLAlchemy data types 


    Parameters
    ----------
    column_name: str 
        The name of the column


    source_datatype: str 
        The pandas data type 

    primary_key: bool
        Is primary key or not 

    
    Returns
    -------
    column: sqlalchemy.Column
        The SQLAlchemy Column
    
    """

    dtype_map = {
        "int64": Integer,
        "object": String,
        "datetime64[ns]": DateTime,
        "float64": Numeric,
        "bool": Boolean
    }

    column = Column(column_name, dtype_map[source_datatype], primary_key=primary_key)

    return column 





def generate_sqlalchemy_schema(df: DataFrame, key_columns: list, table_name, meta: MetaData):

    """
    Generate a sqlalchemy table schema that shall be used to create the target table and perform upserts/inserts
    
    """

    schema = []

    for column in [{"column_name": col[0], "source_datatype": col[1]} for col in zip(df.columns, [dtype.name for dtype in df.dtypes])]:
        schema.append(get_sqlalchemy_column(**column, primary_key=column["column_name"] in key_columns))

    return Table(table_name, meta, *schema)





def upsert_all(df:DataFrame, engine, table_schema:Table, key_columns:list) -> bool:

    """
    Performs the upsert with all rows at once. This may cause timeout issues for large datasets.

    Parameters
    ----------
    df: DataFrame 
        Source data frame 

    engine: PostgreSQL engine 

    table_schema: sqlalchemy.Table 
        The target table 

    key_columns: list 
        The primary keys in the target table 


    Returns
    -------
    bool 

    """
    insert_statement = postgresql.insert(table_schema).values(df.to_dict(orient='records'))
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=key_columns,
        set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})
    result = engine.execute(upsert_statement)
    logging.info(f"Insert/updated rows: {result.rowcount}")
    return True 




def upsert_in_chunks(df:DataFrame, engine, table_schema:Table, key_columns:list, chunksize:int=1000) -> bool:

    """
    Performs the upsert with several rows at a time (i.e. a chunk of rows). 
    This is better suited for very large sql statements that need to be broken into several steps. 


    Parameters
    ----------
    df: DataFrame 
        The data frame 

    engine: PostgreSQL engine 

    table_schema: sqlalchemy.Table 
        Target table metadata 

    key_column: list
        A list of primary key columns in the Table 

    chunksize: int 
        Number of records per chunk 


    Returns
    -------
    bool
    
    """


    max_length = len(df)
    df = df.replace({nan: None})
    for i in range(0, max_length, chunksize):
        if i + chunksize >= max_length: 
            lower_bound = i
            upper_bound = max_length 
        else: 
            lower_bound = i 
            upper_bound = i + chunksize
        insert_statement = postgresql.insert(table_schema).values(df.iloc[lower_bound:upper_bound].to_dict(orient='records'))
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})
        logging.info(f"Inserting chunk: [{lower_bound}:{upper_bound}] out of index {max_length}")
        engine.execute(upsert_statement)
    return True 




    

def upsert_incremental_log(log_path: str, table_name: str, incremental_value) -> bool:


    """

    Watermarking 
        We scan the logs to determine the new data that should be extracted. This functionsaves the logs (incremental value, etc.) for a table at log_path 


    Parameters
    ----------
    log_path: str 
        The directory that contains the log files for a specific table 


    table_name: str 
        Name of the table. Each .sql file is named after the source/target table


    incremental_value: str/int/date 
        The most recent value of the incremental value 




    Returns
    -------
    bool 

    """ 


    # check if the logs of the table exist 
    if f"{table_name}.csv" in os.listdir(log_path):

        # read the file 
        df_existing_incremental_log = read_csv(f"{log_path}/{table_name}.csv")

        # new log 
        df_incremental_log = DataFrame({
            "log_date": [dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")],
            "incremental_value": [incremental_value]

        })

        # combine the old and new logs 
        df_updated_incremental_log = concat([df_existing_incremental_log, df_incremental_log])
        df_updated_incremental_log.to_csv(f"{log_path}/{table_name}.csv", index=False)


    else:

        # if the log doesn't exist then just create it 
        df_incremental_log = DataFrame({
            "log_date": [dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")],
            "incremental_value": [incremental_value]
        })

        # save the new logs 
        df_incremental_log.to_csv(f"{log_path}/{table_name}.csv", index=False)


    return True
