FROM python:3.9-slim

COPY . /prmdata

RUN cd /prmdata && python setup.py install

ENTRYPOINT ["python", "-m", "prmdata.pipeline.main"]
