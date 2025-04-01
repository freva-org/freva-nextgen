"""Read test entries from a databrowser."""
from pathlib import Path

import httpx


async def read_data(core: str, uri: str) -> None:
    """Read mock databrowser data."""
    datapath = Path(__file__).parent.absolute()
    data_json = (datapath / f"{core}.json").read_text().replace("$CWD", str(datapath))
    url = f"{uri}/solr/{core}/update/json?commit=true"
    headers = {"content-type": "application/json"}
    async with httpx.AsyncClient(timeout=5.0) as client:
        res = await client.post(url, content=data_json, headers=headers)
        res.raise_for_status()
