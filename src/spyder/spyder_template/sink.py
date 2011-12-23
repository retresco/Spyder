#
# sink.py 21-Apr-2011
#
"""
Put your storage code here.
"""
from spyder.core.sink import AbstractCrawlUriSink


class MySink(AbstractCrawlUriSink):
    """
    This is my sink.
    """

    def __init__(self, settings):
        """
        Initialize me with some settings.
        """
        pass

    def process_successful_crawl(self, curi):
        """
        We have crawled a uri successfully. If there are newly extracted links,
        add them alongside the original uri to the frontier.
        """
        pass

    def process_not_found(self, curi):
        """
        The uri we should have crawled was not found, i.e. HTTP Error 404. Do
        something with that.
        """
        pass

    def process_redirect(self, curi):
        """
        There have been too many redirects, i.e. in the default config there
        have been more than 3 redirects.
        """
        pass

    def process_server_error(self, curi):
        """
        There has been a server error, i.e. HTTP Error 50x. Maybe we should try
        to crawl this uri again a little bit later.
        """
        pass
