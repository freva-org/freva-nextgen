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

extensions = ["sphinx_code_tabs"]

html_static_path = ["_static"]
html_theme = "furo"
html_logo = os.path.join(html_static_path[0], "logo.png")
templates_path = ["_templates"]
exclude_patterns = []
html_favicon = html_logo
html_theme_options = {
    "navigation_with_keys": True,
    "top_of_page_button": "edit",
}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output
