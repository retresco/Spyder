#
# settings.py
#
"""
Your crawler specific settings.
"""
import logging

LOG_LEVEL_MASTER = logging.INFO
LOG_LEVEL_WORKER = logging.INFO

USER_AGENT = "Mozilla/5.0 (compatible; spyder/0.1; " + \
    "+http://github.com/retresco/spyder)"

# callback for initializing the periodic crawling of the sitemap
MASTER_CALLBACK = 'master.initialize'

# List of positive regular expressions for the crawl scope
REGEX_SCOPE_POSITIVE = [
    "^http://www\.dmoz\.org/Recreation/Boating/Sailing/.*",
]

# List of negative regular expressions for the crawl scope
REGEX_SCOPE_NEGATIVE = [
    "^http://www\.dmoz\.org/Recreation/Boating/Sailing/Racing/.*",
]
