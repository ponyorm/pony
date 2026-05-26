ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim

WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -e . psycopg2-binary pymysql pytest

CMD ["python", "-m", "pytest", "pony/orm/tests/", "-v"]
