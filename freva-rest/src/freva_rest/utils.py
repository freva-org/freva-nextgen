"""Various utilities for the restAPI."""

import os
from typing import Optional

import pika
import redis.asyncio as redis


def str_to_int(inp_str: Optional[str], default: int) -> int:
    """Convert an integer from a string. If it's not working return default."""
    try:
        return int(inp_str)
    except (ValueError, TypeError):
        return default


async def send_borker_message(
    message: bytes, queue: str = "data-portal"
) -> None:
    """Send an alreday encoded message to the message borker."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.environ.get("API_BROKER_HOST", "localhost"),
            port=int(os.environ.get("API_BROKER_PORT", "5672")),
            credentials=pika.PlainCredentials(
                username=os.environ.get("API_BROKER_USER", "rabbit"),
                password=os.environ.get("API_BROKER_PASS", "secret"),
            ),
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange="", routing_key=queue, body=message)
    connection.close()
