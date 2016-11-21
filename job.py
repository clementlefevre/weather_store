from multiprocessing.pool import Pool
import schedule
import time
import log_settings
import logging

from models import DB_tool

from weather_API_service import copy_API_data_to_db


def write_to_db():
    db_tool = DB_tool()
    sites = db_tool.get_sites()
    print "sites found :{}".format(len(sites))
    logging.error("JOB Start")
    try:
        p = Pool(processes=10)
        result = p.map(copy_API_data_to_db, sites)
        p.close()
        p.join()
        logging.info("JOB Finished")
    except Exception as ex:
        template = "An exception of type {0} occured. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logging.error(message)
        logging.error(ex.message)
        raise


def scheduled_task():
    schedule.every(1).hour.do(write_to_db)
    while 1:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    write_to_db()
    scheduled_task()
