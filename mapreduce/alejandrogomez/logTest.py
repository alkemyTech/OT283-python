import logging
from logging import config
from os import path
from time import sleep, perf_counter
from threading import Thread

#   define log_file_config path
log_file_path = path.join(path.dirname(
    path.abspath(__file__)), "logFileConfig.conf")

#   load logs config form log_file_config
config.fileConfig(log_file_path)
#   define test function


def addition(a: int | float, b: int | float) -> int | float:
    """This function executes a plus b

    Args:
        a (int | float): a number to sum
        b (int | float): a number to sum

    Returns:
        int | float: result to sum a plus b
    """
    logging.debug("Inside Addition Function")
    if isinstance(a, str) and a.isdigit():
        logging.warning(
            "Warning : Parameter A is passed as String. Future versions won't support it.")

    if isinstance(b, str) and b.isdigit():
        logging.warning(
            "Warning : Parameter B is passed as String. Future versions won't support it.")

    try:
        result = float(a) + float(b)
        logging.info("Addition Function Completed Successfully")
        return result
    except Exception as e:
        logging.error("Error Type : {}, Error Message : {}".format(
            type(e).__name__, e))
        return None

#   define test function


def task():
    """This function executes and sleeps for one second. Then it executes the second time and also sleeps for 
    another second
    """
    print('Starting a task...')
    sleep(1)
    print('done')


if __name__ == "__main__":
    #logging.info("Current Log Level : {}\n".format(logging.getLevel()))
    a = 10
    b = 20
    result = addition(a, b)
    logging.info("Addition of {} & {} is : {}\n".format(a, b, result))

    a = "20"
    b = 20
    result = addition(a, b)
    logging.info("Addition of {} & {} is : {}\n".format(a, b, 20, result))

    a = "A"
    b = 20
    result = addition(a, b)
    logging.info("Addition of {} & {} is : {}".format(a, b, result))

    start_time = perf_counter()

# create two new threads
    t1 = Thread(target=task)
    t2 = Thread(target=task)

# start the threads
    t1.start()
    t2.start()

# wait for the threads to complete
    t1.join()
    t2.join()

    end_time = perf_counter()

    logging.info(
        f"It took {end_time- start_time: 0.2f} second(s) to complete.")

#   the output showns, the program took one second instead of
#   two to complete
