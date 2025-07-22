# docs/conf.py  ── single source of truth
import os, sys
from importlib.metadata import version as pkg_version

# ── make your package importable ------------------------------------------------
sys.path.insert(0, os.path.abspath(".."))      # repo-root on sys.path

# ── project metadata ------------------------------------------------------------
project   = "ytapi-kit"
author    = "Jake Davis"
copyright = "2025, Jake Davis"

# pull the actual package version so you never update this by hand
release = pkg_version("ytapi_kit")             # e.g. 0.2.1
version = ".".join(release.split(".")[:2])     # 0.2

# ── Sphinx behaviour ------------------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",      # enable .. automodule::, .. autoclass::, …
    "sphinx.ext.napoleon",     # Google / NumPy-style docstrings
    "sphinx.ext.viewcode",     # add “[source]” links
    "sphinx.ext.githubpages",  # writes .nojekyll so Pages serves correctly
    "myst_parser",             # allow Markdown alongside reST
]

templates_path   = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# ── HTML output ----------------------------------------------------------
html_theme       = "sphinx_rtd_theme"
html_theme_options = {
    "collapse_navigation": False,
    "navigation_depth": 2,
    "sticky_navigation": True,
    "titles_only": False,
}
html_static_path = ["_static"]

# optional: nicer defaults for autodoc
extensions += ["sphinx.ext.autosummary"]
autosummary_generate = True
autodoc_default_options = {
    "members": True,
    "autosummary": True,             # ⇐ one line does the magic
    "show-inheritance": True,
}

root_doc = "README"
myst_heading_anchors = 2