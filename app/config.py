import os

# Database configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "storage.db")

# Crawler configuration
CRAWLER_CONFIG = {
    "politeness_delay": 1.0,
    "max_pages": 50,
    "request_timeout": 15,
    "max_workers": 3
}

# Search configuration
SEARCH_CONFIG = {
    "default_limit": 10,
    "snippet_length": 160
}

# Trusted domains
TRUSTED_SUFFIXES = [
    ".gov.in",
    ".nic.in", 
    ".ac.in",
    ".org.in"
]