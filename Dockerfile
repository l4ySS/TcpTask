FROM python:3.13
WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir --upgrade pip
CMD ["python", "server.py"]