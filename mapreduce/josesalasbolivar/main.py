import modules.map_reduce_function as mp_funct
import logging
import os
from os import path
from logging import config
from threading import Thread
from pathlib import Path

# Paths
MAIN_DIR = Path(f"{os.path.dirname(os.path.realpath(__file__))}").parent
CORE_DIR = Path(f"{MAIN_DIR}/core")
FILES_DIR = Path(f"{MAIN_DIR}/files")
DATASETS_DIR = Path(f"{MAIN_DIR}/datasets")

#   define log_file_config path
log_file_path = path.join(path.dirname(
    path.abspath(__file__)), "file_config.conf")

#   load logs config form log_file_config
config.fileConfig(log_file_path)
logger = logging.getLogger("root")


"""Top 10 most viewed post"""


def top_10_most_viewed_post():
    filename = "output_top_view_count.txt"
    out_file_path = f"{FILES_DIR}/{filename}"
    post_view_count_list = mp_funct.top_view_mapper()
    logging.info(f"Instances post view count list")
    mp_funct.top_view_calculator(
        key_value_list=post_view_count_list, out_file_path=out_file_path)
    logging.info(f"Calculating top 10 viewed post ")


"""Top 10 words by tag"""


def top_10_words_by_tag():
    filename = "output_top_words_by_tag.txt"
    out_file_path = f"{FILES_DIR}/{filename}"
    word_by_tag = mp_funct.word_by_tag_mapper()
    logging.info(f"Instances word by tag list")
    word_by_tag = mp_funct.word_by_tag_reducer(key_value_list=word_by_tag)
    logging.info(f"Counting words by tags")
    mp_funct.top_word_by_tag_calculator(
        key_value_list=word_by_tag, out_file_path=out_file_path)
    logging.info(f"Calculating top words by tag")


"""Response Time"""


def response_time():
    key_value_list = mp_funct.response_time_mapper()
    logging.info(f"Instances post list")
    response_time_mean_value = mp_funct.response_time_mean_calculator(
        key_value_list=key_value_list)
    logging.info(f"Calculating response time mean value")
    logging.info(
        f"El tiempo promedio de respuesta para los posts es de {response_time_mean_value} segundos")


"""Main Program"""
if __name__ == "__main__":
    threads = []
    target_list = [top_10_most_viewed_post, top_10_words_by_tag, response_time]

    # Create threads and asign a function for each thread

    for i in range(0, 3):
        function = target_list[i]
        thread = Thread(target=function)
        threads.append(thread)
        logging.info(f"Executing the thread with function {function}")

    # Start all threads
    for thread in threads:
        thread.start()
        logging.info(f"Starting the thread{thread}")

    # Wait for all threads to finish
    # Block the main thread until these threads are finished
    for thread in threads:
        thread.join()
        logging.info(f"Joining the threads {thread}")
