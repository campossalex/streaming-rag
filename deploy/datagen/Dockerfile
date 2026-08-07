# 3.11, not 3.12. kafka-python 2.0.2 -- the version the sibling lab-day scenarios pin -- ships
# a vendored `six` whose `six.moves` shim stopped importing on 3.12, so the producer dies at
# import time with "No module named 'kafka.vendor.six.moves'". Either bump the client or pin
# the interpreter; pinning keeps this identical to the generators in the other scenarios.
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY alarms.py .
# Unbuffered, or the progress lines sit in a pipe buffer and `docker compose logs alarms`
# looks like a hung container.
ENV PYTHONUNBUFFERED=1
CMD ["python", "alarms.py"]
