import logging


LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


# 애플리케이션 기본 로깅 설정을 한 번만 적용한다.
def configure_logging():
    root_logger = logging.getLogger()

    if root_logger.handlers:
        return

    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
    )


# 구조화 로그용 key=value 문자열을 만든다.
def format_log_event(event: str, **fields) -> str:
    parts = [f"event={event}"]

    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")

    return " ".join(parts)
