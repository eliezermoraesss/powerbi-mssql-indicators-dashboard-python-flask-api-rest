FROM python:latest

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY app/ app/
COPY schedule_task.py schedule_task.py

CMD ["python", "app/main.py"]
