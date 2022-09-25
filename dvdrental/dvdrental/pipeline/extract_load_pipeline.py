from dvdrental.elt.extract import extract_from_database 
from dvdrental.elt.load import upsert_to_database
from utility.upsert import is_incremental, get_key_columns
from dvdrental.elt.load import overwrite_to_database, upsert_to_database

class ExtractLoad():

    def __init__(self, source_engine, target_engine, table_name, extract_queries_path:str = "models/extract", path_extract_log: str ="log/extract_log"):

        self.source_engine = source_engine
        self.target_engine = target_engine
        self.table_name = table_name 
        self.extract_queries_path = extract_queries_path 
        self.path_extract_log = path_extract_log


    def run(self):

        df = extract_from_database(
            table_name=self.table_name,
            engine=self.source_engine,
            path=self.extract_queries_path,
            path_extract_log=self.path_extract_log
        )

        # upsert the data 
        # check if the table is incremental
        if is_incremental(table=self.table_name, path=self.extract_queries_path):
            key_columns = get_key_columns(table=self.table_name, path=self.extract_queries_path)
            upsert_to_database(df=df, table_name=self.table_name, key_columns=key_columns, engine=self.target_engine, chunksize=100)
        else:
            overwrite_to_database(df, table_name=self.table_name, engine=self.target_engine)

