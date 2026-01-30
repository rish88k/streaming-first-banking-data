FROM python:3.9

WORKDIR /app

RUN pip install --no-cache-dir psycopg2-binary faker

COPY faker.py .

CMD [ "python", "faker.py" ]