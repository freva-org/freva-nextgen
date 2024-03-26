# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys
from datetime import date

sys.path.insert(0, os.path.abspath(os.path.join("..", "..", "src")))
from databrowser import __version__

project = "Databrowser API"
copyright = f"{date.today().year}, DKRZ"
author = "DKRZ"
release = __version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx_code_tabs",
    "sphinx_copybutton",
    "sphinxcontrib.httpdomain",
]

html_static_path = ["_static"]
# html_theme = "furo"
html_theme = "pydata_sphinx_theme"
html_logo = os.path.join(html_static_path[0], "logo.png")
templates_path = ["_templates"]
exclude_patterns = []
html_favicon = html_logo
html_theme_options = {
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/FREVA-CLINT/databrowserAPI",
            "icon": "fa-brands fa-github",
        }
    ],
    "navigation_with_keys": False,
    "show_toc_level": 4,
    "collapse_navigation": False,
    "navigation_depth": 4,
    "navbar_align": "left",
    "show_nav_level": 4,
    "navigation_depth": 4,
    "navbar_center": ["navbar-nav"],
    "secondary_sidebar_items": ["page-toc"],
}

html_context = {
    "github_user": "FREVA-CLINT",
    "github_repo": "databrowserAPI",
    "github_version": "main",
    "doc_path": "docs",
}
html_sidebars = {"**": ["search-field", "sidebar-nav-bs"]}

# -- Options for autosummary/autodoc output ------------------------------------
autosummary_generate = True
autodoc_typehints = "description"
autodoc_member_order = "groupwise"

# -- Options for autoapi -------------------------------------------------------
autoapi_type = "python"
autoapi_dirs = ["../src/databrowser"]
autoapi_keep_files = True
autoapi_root = "api"
autoapi_member_order = "groupwise"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output
