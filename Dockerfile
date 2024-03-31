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

CMD pipenv run python -m welcome_bot_app \
  --bot-token-file /run/secrets/BOT_TOKEN \
  --telethon-api-id-file /run/secrets/TELETHON_API_ID \
  --telethon-api-hash-file /run/secrets/TELETHON_API_HASH \
  --telethond-session-file /run/secrets/TELETHON_SESSION
