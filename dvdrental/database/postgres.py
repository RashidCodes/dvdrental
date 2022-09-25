import os
from sqlalchemy.engine import URL, create_engine


def create_pg_engine(database: str ="dvdrental"):


    """ 
    Create pg engine 

    
    Parameters
    ----------
    database: str 
        The name of the target database


    Returns
    -------
    PostgreSQL engine

    """ 

    db_user = os.environ.get("db_user")
    db_server_name = os.environ.get("db_server_name")



    # create connection to database 
    connection_url = URL.create(
        drivername = "postgresql+pg8000",
        username = db_user,
        password = "sheed721",
        host = db_server_name,
        port = "5432",
        database = database
    )

    engine = create_engine(connection_url)

    return engine


