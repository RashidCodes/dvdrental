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

    # create connection to database 
    connection_url = URL.create(
        drivername = "postgresql+pg8000",
        username = "postgres",
        password = "",
        host = "localhost",
        port = "5432",
        database = database
    )

    engine = create_engine(connection_url)

    return engine


