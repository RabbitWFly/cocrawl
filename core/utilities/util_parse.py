# _*_ coding: utf-8 _*_


import re
import operator
import functools
import urllib.parse

__all__ = ["get_url_legal"]



def get_url_legal(url, base_url, encoding=None):
    """
    get legal url from a url, based on base_url, and set url_frags.fragment = ""
    :key: http://stats.nba.com/player/#!/201566/?p=russell-westbrook
    """
    url_join = urllib.parse.urljoin(base_url, url, allow_fragments=True)
    url_legal = urllib.parse.quote(url_join, safe="%/:=&?~#+!$,;'@()*[]|", encoding=encoding)
    url_frags = urllib.parse.urlparse(url_legal, allow_fragments=True)
    return urllib.parse.urlunparse((url_frags.scheme, url_frags.netloc, url_frags.path, url_frags.params, url_frags.query, ""))



