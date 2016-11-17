from multiprocessing.pool import Pool
import schedule
import time

from models import DB_tool

from weather_API_service import copy_API_data_to_db


def write_to_db():
    db_tool = DB_tool()
    sites = db_tool.get_sites()
    print "sites found :{}".format(len(sites))
    print "JOB Start"
    try:
        p = Pool(processes=10)
        result = p.map(copy_API_data_to_db, sites)
        p.close()
        p.join()
        print "JOB Finished"
    except Exception as ex:
        template = "An exception of type {0} occured. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print message
        print ex.message
        raise


def job():
    print "coucou {}".format(time.time())


def scheduled_task():
    schedule.every().hour.do(write_to_db)
    while 1:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    write_to_db()
    scheduled_task()
