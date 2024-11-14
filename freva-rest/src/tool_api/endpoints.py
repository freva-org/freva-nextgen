from typing import Dict

from fastapi import Body, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse, StreamingResponse

from freva_rest.auth import TokenPayload, auth
from freva_rest.logger import logger
from freva_rest.rest import app, server_config

from .tool_abstract import ToolAbstract


@app.get("/api/tool/overview", tags=["Tool Overview"])
async def overview() -> JSONResponse:
    """Get all available tools and their attributes."""
    # TODO: Implement method
    tool = ToolAbstract(name="test", 
                        author="JaneDoe",
                        version="0.0",
                        summary="",
                        added="2024-11-13",
                        command="fakecommand",
                        binary="fakebinary",
                        output_type="data"
    )
    output = tool.model_dump()
    return JSONResponse(content=output)

@app.post("/api/tool/add", 
          status_code=status.HTTP_202_ACCEPTED,
          tags=["Tool Addition"])
async def add_tool(tool: ToolAbstract,
                   current_user: TokenPayload = Depends(auth.required)
) -> Dict[str, str]:
    """Add a tool to the MongoDB"""
    # TODO: Implement method
    return {"status": "This method is not implemented yet!"}

@app.delete("/api/tool/del")
async def delete_tool(tool_id:int,
                      current_user: TokenPayload = Depends(auth.required)):
    # TODO: Implement method
    raise NotImplementedError


