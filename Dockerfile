FROM bytewax/bytewax

COPY requirements.txt .

COPY bytewax_materialization_job.py .
COPY dataflow.py .
COPY data/online_store.db data/online_store.db

RUN /venv/bin/pip install -r requirements.txt
