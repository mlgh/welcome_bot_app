from pydantic import BaseModel
from enum import Enum

from welcome_bot_app.model import ChatId
from datetime import timedelta

from welcome_bot_app.safe_html import safe_html_str
from welcome_bot_app import args

args.parser().add_argument(
    "--default-chat-settings-json",
    help="Default chat settings in JSON format.",
    type=str,
)

ICHBIN_REQUEST_HTML = """Hello, $USER!"""

NOT_MUCH_TIME_LEFT_TO_WRITE_ICHBIN_HTML = (
    """You don't have much time left to write a message with $TAG, please do it now!"""
)


WELCOME_HTML = """Welcome, $USER!"""

WELCOME_AGAIN_HTML = """Welcome again, $USER!"""

USER_IS_KICKED_HTML = """$USER didn't write $TAG, so they were kicked."""

INTRODUCTION_TAG = "#ichbin"


class BotReply(BaseModel):
    template_html: str
    ttl: timedelta

    @property
    def template(self) -> safe_html_str:
        return safe_html_str(self.template_html)

    @template.setter
    def template(self, value: safe_html_str) -> None:
        self.template_html = str(value)


class BotReplyType(Enum):
    ICHBIN_REQUEST = "ICHBIN_REQUEST"
    WELCOME = "WELCOME"
    WELCOME_AGAIN = "WELCOME_AGAIN"
    NOT_MUCH_TIME_LEFT_TO_WRITE_ICHBIN = "NOT_MUCH_TIME_LEFT_TO_WRITE_ICHBIN"
    USER_IS_KICKED = "USER_IS_KICKED"


class BotReplies(BaseModel):
    """Replies to different events in the chat."""

    ichbin_request: BotReply = BotReply(
        template_html=ICHBIN_REQUEST_HTML, ttl=timedelta(days=3)
    )
    not_much_time_left_to_write_ichbin: BotReply = BotReply(
        template_html=NOT_MUCH_TIME_LEFT_TO_WRITE_ICHBIN_HTML, ttl=timedelta(days=3)
    )
    welcome: BotReply = BotReply(template_html=WELCOME_HTML, ttl=timedelta(days=3))
    welcome_again: BotReply = BotReply(
        template_html=WELCOME_AGAIN_HTML, ttl=timedelta(minutes=5)
    )
    user_is_kicked: BotReply = BotReply(
        template_html=USER_IS_KICKED_HTML, ttl=timedelta(hours=1)
    )

    def get_reply(self, reply_type: BotReplyType) -> BotReply:
        if reply_type == BotReplyType.ICHBIN_REQUEST:
            return self.ichbin_request
        elif reply_type == BotReplyType.WELCOME:
            return self.welcome
        elif reply_type == BotReplyType.WELCOME_AGAIN:
            return self.welcome_again
        elif reply_type == BotReplyType.NOT_MUCH_TIME_LEFT_TO_WRITE_ICHBIN:
            return self.not_much_time_left_to_write_ichbin
        elif reply_type == BotReplyType.USER_IS_KICKED:
            return self.user_is_kicked


class ChatSettings(BaseModel):
    # Whether the #ichbin feature is enabled in this chat.
    ichbin_enabled: bool = False
    bot_replies: BotReplies = BotReplies()
    # $TAG that must be sent in the introduction message by the user.
    introduction_tag: str = INTRODUCTION_TAG

    # For how long to ban the user if he didn't write ichbin.
    ban_duration: timedelta = timedelta(minutes=1)
    # How long to wait for the user to write ichbin.
    ichbin_waiting_time: timedelta = timedelta(days=3)
    # If the user joined the chat 1 month ago, then rejoined it today, we should not kick him, instead we should give him some grace time to write the ichbin message.
    extra_ichbin_waiting_time_after_rejoining: timedelta = timedelta(hours=1)
    dark_launch_sink_chat_id: ChatId | None = None
    # How long to wait after an unsuccessful kick before trying again.
    failed_kick_retry_time: timedelta = timedelta(hours=1)

    @classmethod
    def get_default(cls) -> "ChatSettings":
        default_chat_settings_json = args.args().default_chat_settings_json
        if default_chat_settings_json is None:
            return ChatSettings()
        else:
            return ChatSettings.model_validate_json(default_chat_settings_json)
