# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
from datetime import date
import pathlib
from freva_client import __version__
import json
import requests

project = "Freva Databrowser"
copyright = f"{date.today().year}, DKRZ"
author = "DKRZ"
release = __version__


def _get_rtd_versions() -> list:
    """Fetch active versions from the ReadTheDocs API."""
    try:
        resp = requests.get(
            "https://readthedocs.org/api/v3/projects/py-oidc-auth/versions/",
            params={"active": True, "limit": 50},
            timeout=5,
        )
        resp.raise_for_status()
        versions = []
        for v in resp.json().get("results", []):
            slug = v["slug"]
            versions.append(
                {
                    "name": slug,
                    "version": slug,
                    "url": f"https://py-oidc-auth.readthedocs.io/en/{slug}/",
                }
            )
        return versions
    except Exception:
        return []  # fail silently so builds don't break offline


def _get_versions() -> list:
    try:
        resp = requests.get(
            "https://api.github.com/repos/freva-org/freva-nextgen/tags",
            timeout=5,
        )
        resp.raise_for_status()
        versions = [
            {
                "name": tag["name"],
                "version": tag["name"],
                "url": f"https://freva-org.github.io/freva-nextgen/{tag['name']}/",
            }
            for tag in resp.json()
            if (
                tag.get("name", "")
                and "dev" not in tag["name"]
                and "rc" not in tag["name"]
            )
        ]
        # Add stable/latest aliases
        versions.insert(
            0,
            {
                "name": "stable",
                "version": "stable",
                "url": "https://freva-org.github.io/freva-nextgen/stable/",
            },
        )
        return versions
    except Exception:
        return []


_switcher_path = pathlib.Path(__file__).parent / "_static" / "switcher.json"
if not os.environ.get("READTHEDOCS"):
    _switcher_path.write_text(json.dumps(_get_rtd_versions(), indent=2))
else:
    _switcher_path.write_text(json.dumps(_get_versions(), indent=2))


# -- General configuration ---------------------------------------------------


extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_code_tabs",
    "sphinx_copybutton",
    "sphinx_togglebutton",
    "sphinxcontrib.httpdomain",
    "sphinx_execute_code",
    "sphinxext.opengraph",
]

napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False


html_static_path = ["_static"]
html_theme = "pydata_sphinx_theme"
html_logo = os.path.join(html_static_path[0], "logo.png")
templates_path = ["_templates"]
exclude_patterns = []
html_favicon = html_logo
html_theme_options = {
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/freva-org/freva-nextgen/freva-client",
            "icon": "fa-brands fa-github",
        }
    ],
    "header_links_before_dropdown": 3,
    "navigation_with_keys": False,
    "show_toc_level": 4,
    "collapse_navigation": False,
    "navigation_depth": 4,
    "navbar_align": "left",
    "show_nav_level": 4,
    "switcher": {
        "json_url": "https://freva-org.github.io/freva-nextgen/stable/_static/switcher.json",
        "version_match": release,
    },
    "navbar_end": ["version-switcher", "navbar-icon-links"],
    "navbar_center": ["navbar-nav"],
    "secondary_sidebar_items": ["page-toc"],
}

html_context = {
    "github_user": "freva-org",
    "github_repo": "freva-nextgen",
    "github_version": "main",
    "doc_path": "docs",
}
html_sidebars = {"**": ["search-field", "sidebar-nav-bs"]}
html_meta = {
    "description": "Freva - the Free Evaluation system framework.",
    "keywords": "freva, climate, data analysis, evaluation, framework, climate science",
    "author": "Freva Team",
    "og:title": "Freva – Free Evaluation System Framework",
    "og:description": "Admin guide for Freva.",
    "og:type": "website",
    "og:url": "https://freva-org.github.io/freva-legacy/",
    "og:image": "https://freva-org.github.io/freva-admin/_images/freva_flowchart-new.png",
    "twitter:card": "summary_large_image",
    "twitter:title": "Freva – Evaluation System Framework",
    "twitter:description": "Search, analyse and evaluate climate model data.",
    "twitter:image": "https://freva-org.github.io/freva-admin/_images/freva_flowchart-new.png",
}

ogp_site_url = "https://freva-org.github.io/freva-legacy"
opg_image = ("https://freva-org.github.io/freva-admin/_images/freva_flowchart-new.png",)
ogp_type = "website"
ogp_custom_meta_tags = [
    '<meta name="twitter:card" content="summary_large_image">',
    '<meta name="keywords" content="freva, climate, data, evaluation, science, reproducibility">',
]


# -- Options for autosummary/autodoc output ------------------------------------
autosummary_generate = True
# autodoc_typehints = "description"
# autodoc_class_signature = "separated"
# autodoc_member_order = "groupwise"


# -- Options for autoapi -------------------------------------------------------
autoapi_type = "python"
autoapi_dirs = ["../src/databrowser"]
autoapi_keep_files = True
autoapi_root = "api"
autoapi_member_order = "groupwise"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# -- MyST options ------------------------------------------------------------

# This allows us to use ::: to denote directives, useful for admonitions
myst_enable_extensions = ["colon_fence", "linkify", "substitution"]
myst_heading_anchors = 2
myst_substitutions = {"rtd": "[Read the Docs](https://readthedocs.org/)"}

# ReadTheDocs has its own way of generating sitemaps, etc.
if not os.environ.get("READTHEDOCS"):
    extensions += ["sphinx_sitemap"]

    html_baseurl = os.environ.get("SITEMAP_URL_BASE", "http://127.0.0.1:8000/")
    sitemap_locales = [None]
    sitemap_url_scheme = "{link}"

# specifying the natural language populates some key tags
language = "en"
