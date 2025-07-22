import os, sys
sys.path.insert(0, os.path.abspath(".."))   # make ytapi_kit importable

extensions = [
    "sphinx.ext.autodoc",     # pull in docstrings
    "sphinx.ext.napoleon",    # understand Google/Numpy style
    "sphinx.ext.viewcode",    # add [source] links
    "sphinx.ext.githubpages", # writes .nojekyll so Pages serves files
    "myst_parser",            # let you write Markdown as well as RST
]

html_theme = "sphinx_rtd_theme"

from importlib.metadata import version as pkg_version
release = pkg_version("ytapi_kit")          # 0.2.1
version = ".".join(release.split(".")[:2])  # 0.2
