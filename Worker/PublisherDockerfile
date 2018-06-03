FROM ubuntu:16.04

RUN apt-get update
# Install Python
RUN apt-get install -y python

# Install pip
RUN apt-get install -y python-pip python-dev build-essential
RUN pip install --upgrade pip
RUN pip install --upgrade virtualenv


# Install app dependencies
RUN pip install pyzmq

WORKDIR /home

COPY ./ZMQHelper.py ./
COPY ./Publisher/PublishData.py ./

CMD ["python", "./PublishData.py"]

EXPOSE 3000