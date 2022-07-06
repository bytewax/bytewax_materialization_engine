FROM bytewax/bytewax

COPY requirements.txt .

COPY bytewax_materialization_job.py .
COPY dataflow.py .

RUN /venv/bin/pip install -r requirements.txt
