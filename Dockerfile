#Generate a base image with python 3.10 with all the dependencies installed

FROM python:3.10-slim as base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt-get update && apt-get install -y gcc
# Set the working directory in the container
WORKDIR /app

COPY pyproject.toml poetry.lock /app/

# Install the dependencies
RUN pip install poetry

# Install the dependencies
RUN poetry config virtualenvs.create false

RUN poetry install --no-interaction

RUN poetry run pip freeze > requirements.txt

FROM python:3.10-slim as dags

WORKDIR /app

COPY . /app

WORKDIR /opt/dagster/app

#copy python dependencies
COPY --from=base /app/requirements.txt /opt/dagster/app/requirements.txt

RUN pip install \
    dagster \
    dagster-postgres \
    dagster-docker

RUN pip install -r requirements.txt

COPY mlb_stats /opt/dagster/app/mlb_stats
COPY mlb_stats_dbt /opt/dagster/app/mlb_stats_dbt
COPY env /opt/dagster/app/env

# Set environment to production by default
ENV ENV=production

EXPOSE 4000

# CMD allows this to be overridden froom run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "--module-name", "mlb_stats"]

# Dagster libraries to run both dagster-webserver and the dagster-daemon. Does not
# need to have access to any pipeline code.

FROM base as dagster_web

#gcc
RUN apt-get update && apt-get install -y gcc

RUN pip install \
    dagster \
    dagster-graphql \
    dagster-webserver \
    dagster-postgres \
    dagster-docker

ENV DAGSTER_HOME=/opt/dagster/app
ENV ENV=production

RUN mkdir -p $DAGSTER_HOME

COPY config/dagster.yaml $DAGSTER_HOME/dagster.yaml
COPY config/workspace.yaml $DAGSTER_HOME/workspace.yaml
COPY env /opt/dagster/app/env

WORKDIR $DAGSTER_HOME
