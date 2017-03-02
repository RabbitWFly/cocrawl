# _*_ coding: utf-8 _*_


import enum
import queue
import logging
import threading
import multiprocessing


class TPEnum(enum.Enum):

    TASKS_RUNNING = "tasks_running"     # flag of tasks_running

    URL_FETCH = "url_fetch"             # flag of url_fetched
    HTM_PARSE = "htm_parse"             # flag of htm_parsed
    ITEM_SAVE = "item_save"             # flag of item_saved

    URL_NOT_FETCH = "url_not_fetch"     # flag of url_not_fetch
    HTM_NOT_PARSE = "htm_not_parse"     # flag of htm_not_parse
    ITEM_NOT_SAVE = "item_not_save"     # flag of item_not_save


class BaseThread(threading.Thread):

    def __init__(self, name, worker, pool):

        threading.Thread.__init__(self, name=name)

        self.worker = worker
        self.pool = pool

    def work(self):

        raise NotImplementedError

    def run(self):
        logging.warning("%s[%s] start", self.__class__.__name__, self.name)
        while True:
            try:
                if not self.work():
                    break
            except queue.Empty:
                if self.pool.is_all_tasks_done():
                    break
        logging.warning("%s[%s] end", self.__class__.__name__, self.name)


class BaseProcess(multiprocessing.Process):

    def __init__(self, name, worker, pool):

        multiprocessing.Process.__init__(self, name=name)

        self.worker = worker
        self.pool = pool

    def work(self):

        raise NotImplementedError

    def run(self):
        logging.warning("%s[%s] start", self.__class__.__name__, self.name)
        while True:
            try:
                if not self.work():
                    break
            except queue.Empty:
                if self.pool.is_all_tasks_done():
                    break
        logging.warning("%s[%s] end", self.__class__.__name__, self.name)

