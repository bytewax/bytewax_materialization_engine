FROM bytewax/bytewax

COPY requirements.txt .

RUN /venv/bin/pip install -r requirements.txt

