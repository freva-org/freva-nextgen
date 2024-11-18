"""The definition of internal endpoints used to communicate with running tools."""

from typing import Annotated

from fastapi import Path, WebSocket

from freva_rest.rest import app

__all__ = ["tool_communication"]


@app.websocket(
    "/api/freva-nextgen/tool_internal/ws/{uuid}/{client_secret}",
)
async def tool_communication(
    websocket: WebSocket,
    uuid: Annotated[str, Path(description="UUID of the running tool.")],
    client_secret: Annotated[str, Path(description="Internal user id.")],
) -> None:
    """This internal websocket endpoint communicates with the running tool.

    The running tool establishes a connection to the server via this websocket.
    Data such as logs, scaled images and videos are then transferred to the
    server via the websocket.

    Business Logic:
        - Check if client allowed to make a connection, I think this could be
          done by defining a secret on statart up of the server and passing this
          to the script that gets executed so that it can pass it back to this
          endpoint. If this fails: 404
        - Read the DB entry and get all the information needed to create
          the logs and create those logs. If that fails: 500
        - Write incoming log messages to disk.
        - Write incoming images to the disk.
        - Mark the job as done when the told to do so.
    """
