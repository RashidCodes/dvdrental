import datetime as dt 
import numpy as np 
from pandas import read_csv



# get a csv from a folder called extract_log by default 
def get_incremental_value(table_name: str, path: str="extract_log"):

    """

    Water-marking 
        This is use to keep track of previously extracted data 


    Parameters
    ----------
    table_name: str 
        Name of the table we want to inspect 


    path: str 
        Location of the table 




    Returns
    -------
    incremental_value: str/date/int
        Most recent record extracted


    """ 

    df = read_csv(f"{path}/{table_name}.csv")


    # get the most recent log date 
    recent_date = df["log_date"].max()

    # get the incremental value of the record with the most recent date 
    incremental_value = df[df["log_date"] == recent_date]["incremental_value"].values[0]

    return incremental_value

