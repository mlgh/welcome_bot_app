# Use Python 3.11 official base image
FROM python:3.11

# Create venv inside working directory.
ENV PIPENV_VENV_IN_PROJECT=1
# Keeps Python from generating .pyc files in the container.
ENV PYTHONDONTWRITEBYTECODE=1
# Turns off buffering for easier container logging.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN python -m pip install --upgrade pip
RUN pip install pipenv

# Install python dependencies.
COPY Pipfile.lock Pipfile ./
COPY src/ src/
RUN pipenv install --deploy --ignore-pipfile

CMD exec pipenv run python -m welcome_bot_app \
  --bot-token-file /run/secrets/BOT_TOKEN \
  --event-queue-file /welcome_app/db/queue.db \
  --event-log-file /welcome_app/db/log.db \
  --storage-url=sqlite:////welcome_app/db/users.db
