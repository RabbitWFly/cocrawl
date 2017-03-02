# _*_ coding: utf-8 _*_


import time
import logging
from .concur_base import BaseThread, BaseProcess, TPEnum


# ===============================================================================================================================
def work_fetch(self):

    # ----1
    priority, url, keys, deep, fetch_repeat = self.pool.get_a_task(TPEnum.URL_FETCH)

    # ----2
    code, content = self.worker.working(url, keys, fetch_repeat)

    # ----3
    if code > 0:
        self.pool.update_number_dict(TPEnum.URL_FETCH, +1)
        self.pool.add_a_task(TPEnum.HTM_PARSE, (priority, url, keys, deep, content))
    elif code == 0:
        self.pool.add_a_task(TPEnum.URL_FETCH, (priority+1, url, keys, deep, fetch_repeat+1))
    else:
        pass

    # ----4
    self.pool.finish_a_task(TPEnum.URL_FETCH)
    return True

FetchThread = type("FetchThread", (BaseThread,), dict(work=work_fetch))
FetchProcess = type("FetchProcess", (BaseProcess,), dict(work=work_fetch))


# ===============================================================================================================================
def work_parse(self):

    # ----1
    priority, url, keys, deep, content = self.pool.get_a_task(TPEnum.HTM_PARSE)

    # ----2
    code, url_list, save_list = self.worker.working(priority, url, keys, deep, content)

    # ----3
    if code > 0:
        self.pool.update_number_dict(TPEnum.HTM_PARSE, +1)
        for _url, _keys, _critical, _priority in url_list:
            self.pool.add_a_task(TPEnum.URL_FETCH, (_priority, _url, _keys, deep+1, 0))
        for item in save_list:
            self.pool.add_a_task(TPEnum.ITEM_SAVE, (url, keys, item))


    # ----4
    self.pool.finish_a_task(TPEnum.HTM_PARSE)
    return True

ParseThread = type("ParseThread", (BaseThread,), dict(work=work_parse))
ParseProcess = type("ParseProcess", (BaseProcess,), dict(work=work_parse))


# ===============================================================================================================================
def work_save(self):

    # ----1
    url, keys, item = self.pool.get_a_task(TPEnum.ITEM_SAVE)

    # ----2
    result = self.worker.working(url, keys, item)

    # ----3
    if result:
        self.pool.update_number_dict(TPEnum.ITEM_SAVE, +1)

    # ----4
    self.pool.finish_a_task(TPEnum.ITEM_SAVE)
    return True

SaveThread = type("SaveThread", (BaseThread,), dict(work=work_save))
SaveProcess = type("SaveProcess", (BaseProcess,), dict(work=work_save))


# ===============================================================================================================================
def init_monitor_thread(self, name, pool, sleep_time=5):

    BaseThread.__init__(self, name, None, pool)

    self.sleep_time = sleep_time    # sleeping time in every loop
    self.init_time = time.time()    # initial time of this spider

    self.last_fetch_num = 0         # fetch number in last time
    self.last_parse_num = 0         # parse number in last time
    self.last_save_num = 0          # save number in last time
    return


def work_monitor(self):

    time.sleep(self.sleep_time)
    cur_fetch_num = self.pool.number_dict[TPEnum.URL_FETCH]
    cur_parse_num = self.pool.number_dict[TPEnum.HTM_PARSE]
    cur_save_num = self.pool.number_dict[TPEnum.ITEM_SAVE]

    info = "%s status: running_tasks=%s;" % (self.pool.pool_name, self.pool.number_dict[TPEnum.TASKS_RUNNING])
    info += " fetch=(%d, %d, %d/(%ds));" % \
            (self.pool.number_dict[TPEnum.URL_NOT_FETCH], cur_fetch_num, cur_fetch_num-self.last_fetch_num, self.sleep_time)
    info += " parse=(%d, %d, %d/(%ds));" % \
            (self.pool.number_dict[TPEnum.HTM_NOT_PARSE], cur_parse_num, cur_parse_num-self.last_parse_num, self.sleep_time)
    info += " save=(%d, %d, %d/(%ds));" % \
            (self.pool.number_dict[TPEnum.ITEM_NOT_SAVE], cur_save_num, cur_save_num-self.last_save_num, self.sleep_time)
    info += " total_seconds=%d" % (time.time() - self.init_time)
    logging.warning(info)

    self.last_fetch_num = cur_fetch_num
    self.last_parse_num = cur_parse_num
    self.last_save_num = cur_save_num

    return False if self.pool.monitor_stop else True

MonitorThread = type("MonitorThread", (BaseThread,), dict(__init__=init_monitor_thread, work=work_monitor))
