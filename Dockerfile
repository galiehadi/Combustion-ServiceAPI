FROM python:3.7.7-slim

COPY requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip install -r requirements.txt

COPY . /app

CMD [ "python3", "./CombustionAPI.py" ]

EXPOSE 8083