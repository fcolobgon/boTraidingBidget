import io
import os
from pathlib import Path
from setuptools import setup, find_packages


# ********************************
# Configuration
# ********************************

APP_ROOT = Path(__file__).parent
PATH = os.path.abspath(os.path.dirname(__file__))

REQUIREMENTS_FILE = "requirements.txt"

EXTRA_REQUIREMENTS_FILE = "test-requirements.txt"

PACKAGES_EXCLUDE = ["tests", "*.tests", "*.tests.*", "tests.*", "docs", "examples"]

# ********************************
# Package meta-data default values
# ********************************

NAME = "iam-apy-py"
DESCRIPTION = (
    "xxxxx "
    "xxxxx"
)
VERSION = "0.0.1"
AUTHOR = "atSistemas"
PROJECT_URLS = {
    "Documentation": "xxx",
    "Bug Tracker": "xxx",
    "Source Code": "xxx",
}
REQUIRES_PYTHON = ">=3.8"
INSTALL_REQUIRES = [
    "Flask"
]
EXTRAS_REQUIRE = {
    "dev": [
        "black",
        "flake8",
        "pre-commit",
        "pydocstyle",
        "pytest",
        "pytest-black",
        "pytest-clarity",
        "pytest-dotenv",
        "pytest-flake8",
        "pytest-flask",
        "tox",
    ]
}

# ********************************
# Package meta-data dynamic values
# ********************************

# Prepare info dictionary
info_dict = {}

# Load version -> the package's __version__.py module as a info dictionary
if not VERSION:
    VERSION = (APP_ROOT / "__version__.py").read_text()

info_dict["VERSION"] = VERSION

info_dict["README"] = (APP_ROOT / "README.md").read_text()

info_dict["LICENSE"] = (APP_ROOT / "LICENSE").read_text()

# *****
# Setup
# *****

setup(
    name=NAME,
    description=DESCRIPTION,
    long_description=info_dict["README"],
    long_description_content_type="text/markdown",
    version=info_dict["VERSION"],
    author=AUTHOR,
    maintainer=AUTHOR,
    license=info_dict["LICENSE"],
    url="XXX",
    project_urls=PROJECT_URLS,
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    packages=find_packages("src", exclude=PACKAGES_EXCLUDE),
    python_requires=REQUIRES_PYTHON,
    install_requires=[i.strip() for i in open(REQUIREMENTS_FILE).readlines()],
    extras_require=[i.strip() for i in open(EXTRA_REQUIREMENTS_FILE).readlines()],
    scripts=[],
)