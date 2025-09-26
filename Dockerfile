FROM python:3.11-slim

WORKDIR /app

COPY mongo_query_generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mongo_query_generator/ .

EXPOSE 7323

CMD ["python", "main.py"]


