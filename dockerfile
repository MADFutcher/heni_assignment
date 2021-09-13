# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN apt update && apt install sqlite3
RUN pip3 install -r requirements.txt


COPY . .

CMD ["heni.py"]

ENTRYPOINT ["python3"]
