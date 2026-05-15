ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim

WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -e .

CMD ["python", "-m", "unittest", "discover", "-v", "-s", "pony/orm/tests", "-p", "test_*.py"]
