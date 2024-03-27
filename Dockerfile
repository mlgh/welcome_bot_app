# Use Python 3.11 official base image
FROM python:3.11

WORKDIR /app

# Create venv inside working directory.
ENV PIPENV_VENV_IN_PROJECT=1
# Keeps Python from generating .pyc files in the container.
ENV PYTHONDONTWRITEBYTECODE=1
# Turns off buffering for easier container logging.
ENV PYTHONUNBUFFERED=1

COPY Pipfile.lock Pipfile ./

RUN python -m pip install --upgrade pip
RUN pip install pipenv

# Install python dependencies.
RUN pipenv install --deploy --ignore-pipfile