"""Definition of routes for authentication."""

import os
from typing import Any, Dict, List, Optional

import aiohttp
import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2AuthorizationCodeBearer
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel

from freva_rest.rest import app, server_config

"__all__" == ["login_for_access_token"]

TIMEOUT: aiohttp.ClientTimeout = aiohttp.ClientTimeout(total=5)
"""5 seconds for timeout for key cloak interaction."""


class Token(BaseModel):
    """Model representing an OAuth2 token response from Keycloak."""

    access_token: str
    token_type: str
    expires_in: int
    refresh_token: str
    refresh_expires_in: int


async def get_token(username: str, password: str) -> Token:
    """
    Retrieve an OAuth2 token from Keycloak using the Resource Owner Password Credentials grant type.

    Parameters
    -----------

    username (str):
        The username of the user.
    password (str):
        The password of the user.

    Returns:
        Token: An object containing the access token and related information.

    Raises:
        HTTPException: If the response from Keycloak is not successful.
    """
    data = {
        "client_id": os.getenv("KEYCLOAK_CLIENT_ID"),
        "client_secret": os.getenv("KEYCLOAK_CLIENT_SECRET"),
        "grant_type": "password",
        "username": username,
        "password": password,
    }

    async with aiohttp.ClientSession(timeout=TIMEOUT) as client:
        response = await client.post(
            f"{server_config.keycloak_auth_url}/token", data=data
        )
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail="Invalid username or password",
            )
        return Token(**response.json())


oauth2_scheme = OAuth2AuthorizationCodeBearer(
    authorizationUrl=f"{server_config.keycloak_auth_url}/auth",
    tokenUrl=f"{server_config.keycloak_auth_url}/token",
    refreshUrl=f"{server_config.keycloak_auth_url}/token",
)


class TokenPayload(BaseModel):
    """Model representing the payload of a JWT token."""

    sub: str
    exp: int
    email: Optional[str] = None


async def get_jwks() -> List[Dict[str, Any]]:
    """Retrieve the JSON Web Key Set (JWKS) from Keycloak.

    Returns
    -------
    List[Dict[str, Any]]: The list of public keys.

    Raises
    ------
    HTTPException: If the request to Keycloak fails.
    """
    async with aiohttp.ClientSession(timeout=TIMEOUT) as client:
        response = await client.get(server_config.keycloak_discovery_url)
        response.raise_for_status()
        jwks_uri = response.json()["jwks_uri"]

        response = await client.get(jwks_uri)
        response.raise_for_status()
        return response.json()["keys"]


def get_public_key(jwks: List[Dict[str, Any]], kid: str) -> str:
    """Extract the public key from the JWKS that matches the key ID (kid).

    Parameters
    ---------
    jwks (List[Dict[str, Any]]):
        The list of public keys.
    kid (str):
        The key ID to match.

    Returns
    -------
    str: The public key as a PEM-formatted string.

    Raises
    ------
    HTTPException:
        If no matching key is found.
    """

    for key in jwks:
        if key["kid"] == kid:
            return f"-----BEGIN PUBLIC KEY-----\n{key['n']}\n-----END PUBLIC KEY-----"
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Public key not found",
    )


async def get_current_user(
    token: str = Depends(oauth2_scheme),
) -> TokenPayload:
    """Get the current user by verifying the JWT token.

    Parameters
    ----------
    token (str):
        The JWT token from the Authorization header.

    Returns
    -------
    TokenPayload: The payload data from the token.

    Raises
    ------
    HTTPException: If the token is invalid or verification fails.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        jwks = await get_jwks()
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header["kid"]
        public_key = get_public_key(jwks, kid)
        payload = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            audience=os.getenv("KEYCLOAK_CLIENT_ID", ""),
        )
        token_data = TokenPayload(**payload)
    except jwt.exceptions.PyJWTError:
        raise credentials_exception from None

    return token_data


@app.post("/api/auth/v2/token/", response_model=Token, tags=["Authentication"])
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
) -> Token:
    """Create an new login token."""
    return await get_token(form_data.username, form_data.password)
