FROM bytewax/bytewax as build

COPY requirements.txt .

RUN /venv/bin/pip install -r requirements.txt

FROM build

COPY bytewax_materialization_dataflow.py .
COPY dataflow.py .
COPY data/online_store.db data/online_store.db


