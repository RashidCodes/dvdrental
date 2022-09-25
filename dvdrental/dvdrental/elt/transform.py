import os
import jinja2 as j2 
import logging


class Transform():

    def __init__(self, model, engine, models_path="models"):
        self.model = model 
        self.engine = engine 
        self.models_path = models_path 


    def run(self) -> bool:

        """

        Run models in models directory 


        Parameters
        ----------
        model: str 
            The name of the model (withouth .sql) 

        engine
            Postgres engine 

        models_path: str 
            Directory containing the models 


        
        Returns
        --------
        bool

        """ 

        logging.basicConfig(format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s")


        if f"{self.model}.sql" in os.listdir(self.models_path):
            logging.info(f"Building model: {self.model}")

            # read sql contents into a variable 
            with open(f"{self.models_path}/{self.model}.sql") as f:
                raw_sql = f.read() 


            # parse the sql using jinja 
            rendered = j2.Template(raw_sql).render(target_table=self.model, engine=self.engine)

            # execute parsed sql 
            self.engine.execute(rendered)

            logging.info(f"Successfully build model: {self.model}")
            return True 

        else:
            logging.error(f"Could not find model: {self.model}")
            return False 

    



