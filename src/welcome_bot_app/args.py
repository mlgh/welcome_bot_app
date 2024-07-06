import argparse

_GLOBAL_PARSER: argparse.ArgumentParser | None = argparse.ArgumentParser(
    description="Welcoming Telegram bot."
)
_GLOBAL_ARGS: argparse.Namespace | None = None


def parser() -> argparse.ArgumentParser:
    global _GLOBAL_PARSER
    assert _GLOBAL_PARSER is not None, "parser() called after args()"
    return _GLOBAL_PARSER


def args() -> argparse.Namespace:
    global _GLOBAL_ARGS
    global _GLOBAL_PARSER
    if _GLOBAL_ARGS is None:
        assert _GLOBAL_PARSER is not None, "create_parser was not called before args()"
        _GLOBAL_ARGS = _GLOBAL_PARSER.parse_args()
        _GLOBAL_PARSER = None
    return _GLOBAL_ARGS
