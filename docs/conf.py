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
