FROM python:3.11-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Gateway uses 8000 inside the container; compose maps 8001/8002 on the host.
EXPOSE 8000

CMD ["uvicorn", "app.gateway_app:app", "--host", "0.0.0.0", "--port", "8000"]
