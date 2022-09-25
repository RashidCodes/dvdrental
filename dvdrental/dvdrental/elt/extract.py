import os 
import logging 
import jinja2 as j2
from pandas import DataFrame, read_sql
from utility.upsert import upsert_incremental_log 
from utility.get_incremental_log_value import get_incremental_value


def extract_from_database(table_name, engine, path="extract_queries", path_extract_log="extract_log") -> DataFrame:

    """ 

    Extract data from a database


    Parameters
    ----------
    table_name: str 
        The name of the source table 


    engine: Postgres engine 


    path: str 
        Directory containing the extract queries.




    Returns
    -------
    extracted_df: DataFrame 
        Data frame containing data from the source table


    """

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")

    if f"{table_name}.sql" in os.listdir(path):

        logging.info(f"Extracting table: {table_name}")

        # read the sql contents 
        with open(f"{path}/{table_name}.sql") as f:
            raw_sql = f.read()

        
        
        # get the config from the extract query
        config  = j2.Template(raw_sql).make_module().config


        # get the config 
        if config["extract_type"].lower() == "incremental":


            if not os.path.exists(path_extract_log):
                os.mkdir(path_extract_log)


            # get the watermark files for the table 
            if f"{table_name}.csv" in os.listdir(path_extract_log):

                # get the incremental value and perform the incremental extract 
                current_max_incremental_value = get_incremental_value(table_name, path=path_extract_log)

                parsed_sql = j2.Template(raw_sql).render(
                    source_table = table_name,
                    engine =  engine,
                    is_incremental = True,
                    incremental_value = current_max_incremental_value
                )


                # execute incremental extraction 
                extracted_df = read_sql(parsed_sql, con=engine)


                # update max incremental extract 
                if extracted_df.shape[0] > 0:
                    max_incremental_value = extracted_df[config["incremental_column"]].max()

                else:
                    max_incremental_value = current_max_incremental_value 



                # upsert the logs 
                upsert_incremental_log(log_path=path_extract_log, table_name=table_name, incremental_value=max_incremental_value)
                logging.info(f"Successfully extracted table: {table_name} incrementally, rows extracted: {extracted_df.shape[0]}")

                return extracted_df

            else:

                # parse sql using jinja 
                parsed_sql = j2.Template(raw_sql).render(source_table = table_name, engine = engine)

                # perform full extract (first time only)
                df = read_sql(parsed_sql, con=engine)

                # store the latest incremental value 
                max_incremental_value = df[config["incremental_column"]].max()

                upsert_incremental_log(log_path=path_extract_log, table_name=table_name, incremental_value=max_incremental_value)

                logging.info(f"Successfully extracted table: {table_name}, rows extracted: {df.shape[0]}")

                return df

        else:

            # parse sql with jinja
            parsed_sql = j2.Template(raw_sql).render(source_table=table_name, engine=engine)

            # store extracted data 
            extracted_df = read_sql(parsed_sql, con=engine)

            logging.info(f"Successfully extracted table: {table_name}, rows extracted: {extracted_df.shape[0]}")

            return extracted_df 


    else:

        logging.error(f"Could not find table: {table_name}")

