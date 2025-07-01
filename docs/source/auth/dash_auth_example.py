import os
from typing import Any, Dict, Optional

import dash
import plotly.express as px
import requests
from dash import Dash, Input, Output, dcc, html
from flask import Flask, Response, redirect, request, session

# =============================
# CONFIGURATION
# =============================

FREVA_API_BASE: str = "https://freva.dkrz.de/api/freva-nextgen/auth/v2"
REDIRECT_URI: str = "http://localhost:8050/callback"
LOGIN_ENDPOINT: str = f"{FREVA_API_BASE}/login"
TOKEN_ENDPOINT: str = f"{FREVA_API_BASE}/token"
USERINFO_ENDPOINT: str = f"{FREVA_API_BASE}/userinfo"

# Initialize Flask server for Dash backend
server: Flask = Flask(__name__)

# ⚠️ SECURITY WARNING:
# This key signs the session cookie. In production, use a secure env var:
# e.g., server.secret_key = os.environ["FLASK_SECRET_KEY"]
server.secret_key = os.urandom(24)

# =============================
# DASH APP INITIALIZATION
# =============================

app: Dash = dash.Dash(
    __name__, server=server, use_pages=False, suppress_callback_exceptions=True
)
app.title = "Freva OIDC Dash Example"

# =============================
# FLASK AUTH ROUTES
# =============================


@server.route("/login")
def login() -> Response:
    """Redirects user to Freva's OIDC login page."""
    return redirect(f"{LOGIN_ENDPOINT}?redirect_uri={REDIRECT_URI}")


@server.route("/callback")
def callback() -> Response:
    """Handles OIDC redirect, exchanges code for token, and stores in session."""
    code: Optional[str] = request.args.get("code")
    state: Optional[str] = request.args.get("state")

    if not code or not state:
        return Response("Missing code or state", status=400)

    try:
        # Exchange authorization code for token
        token_res = requests.post(
            TOKEN_ENDPOINT,
            data={
                "code": code,
                "redirect_uri": REDIRECT_URI,
                "grant_type": "authorization_code",
            },
        )
        token_res.raise_for_status()
        tokens: Dict[str, Any] = token_res.json()
        session["access_token"] = tokens["access_token"]

        # Retrieve user info
        userinfo_res = requests.get(
            USERINFO_ENDPOINT,
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        userinfo_res.raise_for_status()
        session["userinfo"] = userinfo_res.json()

    except requests.RequestException as e:
        return Response(f"Login failed: {str(e)}", status=500)

    return redirect("/")


@server.route("/logout")
def logout() -> Response:
    """Clears the session and logs user out."""
    session.clear()
    return redirect("/")


# =============================
# DASH LAYOUT AND CALLBACK
# =============================

app.layout = html.Div([dcc.Location(id="url", refresh=True), html.Div(id="main")])


@app.callback(Output("main", "children"), Input("url", "pathname"))
def render_content(_: str) -> Any:
    """Main page content: authenticated view or redirect to login."""
    if "userinfo" not in session:
        return dcc.Location(href="/login", id="redirect")

    user: str = session["userinfo"].get("username", "Unknown")
    access_token: str = session["access_token"]

    fig = px.scatter(x=[1, 2, 3], y=[3, 1, 6], title="Freva Demo Scatter")

    return html.Div(
        [
            html.H2(f"Welcome, {user}"),
            html.P("Your OAuth2 token (access_token) is:"),
            html.Pre(
                access_token,
                style={"whiteSpace": "pre-wrap", "wordBreak": "break-all"},
            ),
            dcc.Graph(figure=fig),
            html.Br(),
            html.A("Logout", href="/logout"),
        ]
    )


# =============================
# LOCAL ENTRY POINT (FOR DEBUG)
# =============================

if __name__ == "__main__":
    # Use `gunicorn main:server` for production deployment
    app.run(debug=True)
