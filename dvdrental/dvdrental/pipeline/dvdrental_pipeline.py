import os 
import logging
from database.postgres import create_pg_engine
from dvdrental.elt.transform import Transform
from graphlib import TopologicalSorter
from dvdrental.pipeline.extract_load_pipeline import ExtractLoad
from sqlalchemy.exc import ProgrammingError


def run_pipeline():

    completed_without_errors = True

    # configure pipeline 
    path_extract_model = "models/extract"
    path_transform_model = "models/transform"

    # set the target database
    target_database = os.environ.get("db_target_database_name")

    # configure engines
    source_engine = create_pg_engine()
    target_engine = create_pg_engine(target_database)

    dag = TopologicalSorter()
    nodes_extract_load = []


    for file in os.listdir(path_extract_model):
        node_extract_load = ExtractLoad(source_engine, target_engine, table_name=file.replace(".sql", ""))
        dag.add(node_extract_load) 
        nodes_extract_load.append(node_extract_load)


 
    # transform nodes 
    node_staging_films = Transform("staging_films", engine=target_engine, models_path=path_transform_model)
    node_serving_films_popular = Transform("serving_films_popular", engine=target_engine, models_path=path_transform_model)

    # create dags 
    dag.add(node_staging_films, *nodes_extract_load)
    dag.add(node_serving_films_popular, *nodes_extract_load)

    dag_rendered = tuple(dag.static_order())

    for node in dag_rendered:
        try:
            node.run()
        except AttributeError as err:
            logging.warning(f'TemplateModule object - {file} has no attribute config')
            completed_without_errors = False
            continue
        except ProgrammingError as err:
            logging.error(f'An error occurred: {err}')
            completed_without_errors = False
            continue
        except FileNotFoundError as err:
            logging.error(f"File not found: {err}")
            completed_without_errors = False
            continue

            


    return completed_without_errors





if __name__ == '__main__':

    if run_pipeline():
        print("Successfully loaded DVD Rental Data")
    else:
        print("Pipeline completed with errors. See logs")

