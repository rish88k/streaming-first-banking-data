FROM python:3.9

WORKDIR /app

RUN pip install --no-cache-dir psycopg2-binary faker

COPY seed_data.py .

CMD [ "python", "seed_data.py" ]