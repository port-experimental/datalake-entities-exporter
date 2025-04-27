FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1


RUN apt-get update \
    && apt-get install -y \
        ca-certificates \
        openssl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /app

RUN pip install -U poetry

RUN poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock ./

RUN poetry install --without dev --no-root --no-interaction --no-ansi --no-cache

COPY . .

ENTRYPOINT ["poetry", "run", "python", "main.py"]
