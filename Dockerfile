FROM python:3.7-slim

RUN apt-get update && \
    apt-get install -y curl

COPY requirements.txt /xero/requirements.txt
RUN pip install -r /xero/requirements.txt

RUN mkdir -p "xero"
WORKDIR "/xero"

EXPOSE 8050
CMD ["python", "-m", "xero_dash"]
