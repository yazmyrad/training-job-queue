FROM python:3.12-slim

WORKDIR /training-job-queue

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src ./src

CMD ["python", "-m", "src.worker"]