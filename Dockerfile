FROM python:3.9

WORKDIR /src 

COPY /dvdrental .

RUN pip install -r requirements.txt 

ENV PYTHONPATH=/src 

CMD ["python", "dvdrental/pipeline/dvdrental_pipeline.py"]
