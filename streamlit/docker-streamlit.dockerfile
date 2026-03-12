FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/.streamlit

COPY banking_app.py banking_app.py

EXPOSE 8501

CMD ["streamlit", "run", "banking_app.py", "--server.port=8501", "--server.address=0.0.0.0"]