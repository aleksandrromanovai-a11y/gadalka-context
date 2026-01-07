"""Main entrypoint with message processing logic for the FastAPI/uvicorn server."""

from typing import Any, Dict

from app import create_app, run_server
from log.logger import get_logger
from message_handler import build_message_processor

logger = get_logger(__name__)
processor = build_message_processor()


def process_message(message: str, metadata: Dict[str, Any]) -> str:
    """API entrypoint uses the same get_response pipeline as Kafka worker."""
    return processor(message, metadata or {}) or ''


# Expose app for `uvicorn main:app --reload`
app = create_app(process_message)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description='Run the simple text/JSON echo server (uvicorn).'
    )
    parser.add_argument('--host', default='0.0.0.0', help='Host interface to bind.')
    parser.add_argument('--port', type=int, default=8000, help='Port to listen on.')
    args = parser.parse_args()
    processor = build_message_processor()

    run_server(host=args.host, port=args.port, message_processor=processor)


if __name__ == "__main__":
    main()
