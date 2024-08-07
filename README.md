Running the bot locally
----

```
git clone https://github.com/mlgh/welcome_bot_app.git
cd welcome_bot_app
pipenv install
mkdir secrets
```

- populate `secrets/BOT_TOKEN` with the token from the @BotFather.
- populate `secrets/TELETHON_API_ID` and `secrets/TELETHON_API_HASH` using instructions from https://docs.telethon.dev/en/stable/basic/signing-in.html

If you are running the first time, Telethon would need to authorize you in order to create a session file, please follow the instructions in the terminal.

`--telethon-*` arguments are optional if you don't need Telethon functionality.

```
rm -rf /tmp/bot_dbs ;
mkdir /tmp/bot_dbs ;
pipenv run python3 -m welcome_bot_app \
 --bot-token-file secrets/BOT_TOKEN \
 --telethon-api-id-file secrets/TELETHON_API_ID \
 --telethon-api-hash-file secrets/TELETHON_API_HASH \
 --telethon-session-file-prefix secrets/TELETHON_SESSION \
 --event-queue-file /tmp/bot_dbs/queue.db\
 --event-log-file /tmp/bot_dbs/log.db \
 --storage-url=sqlite:////tmp/bot_dbs/users.db \
 --default-chat-settings-json '{"ichbin_waiting_time": "PT30S", "ichbin_enabled": false}'
```

Static analysis
----

```
pipenv run mypy src/welcome_bot_app
pipenv run ruff check --fix
pipenv run ruff format
```

Precommit hooks
----
```
pipenv run pre-commit
```
