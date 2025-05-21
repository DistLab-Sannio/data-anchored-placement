FROM python:3.11-bookworm

WORKDIR /root

ADD . .

RUN pip install -r requirements.txt


#ENTRYPOINT ["top", "-b"]
ENTRYPOINT ["python", "scheduler.py"]
