FROM python:3.7.6-slim-buster

WORKDIR /app

COPY ./kafka/requirements.txt /app/

RUN pip install -r requirements.txt

COPY ./kafka/ /app/
COPY ./config.py /app/

ENTRYPOINT [ "/app/entrypoint.sh" ]