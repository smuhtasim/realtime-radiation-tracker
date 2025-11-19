FROM apache/flink:1.17

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install apache-flink==1.17 pandas && \
    ln -s /usr/bin/python3 /usr/bin/python

COPY requirements.txt /opt/
RUN pip install -r /opt/requirements.txt

WORKDIR /opt/pyflink_job

COPY job.py .
COPY jars /opt/flink/usrlib

CMD ["python", "job.py"]