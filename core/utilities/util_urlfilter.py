# _*_ coding: utf-8 _*_

import pybloom_live



class UrlFilter(object):

    def __init__(self, capacity=None):

        self.url_set = set() if not capacity else None
        self.bloom_filter = pybloom_live.ScalableBloomFilter(capacity, error_rate=0.001) if capacity else None


    def update(self):
        """
        update this urlfilter, you can rewrite this function if necessary
        """
        raise NotImplementedError

    def check(self, url):
        """
        check the url to make sure that the url hasn't been fetched
        """

        result = False

        if self.url_set is not None:
            result = False if url in self.url_set else True
            self.url_set.add(url)
        elif self.bloom_filter is not None:
            # bloom filter, "add": if key already exists, return True, else return False
            result = (not self.bloom_filter.add(url))
        else:
            pass

        return result
