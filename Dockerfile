ARG PYTHON_VERSION=3.13
FROM python:${PYTHON_VERSION}-slim

WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -e .

CMD ["sh", "-c", "python -m unittest discover -v -s pony/orm/tests -p test_*.py && python pony/orm/examples/run_examples.py"]
