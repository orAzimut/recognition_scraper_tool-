FROM python:3.13.3

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY resources/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/output_json

RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

# Change to run the scheduled version
CMD ["python", "main_scheduled.py"]