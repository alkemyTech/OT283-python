import dateutil.parser
import datetime
import os
import operator
import re
import statistics as sts
import lxml.html as LH
import xml.etree.ElementTree as ET
import logging
from logging import config
from pathlib import Path

# Paths
MAIN_DIR = Path(f"{os.path.dirname(os.path.realpath(__file__))}").parent.parent
CORE_DIR = Path(f"{MAIN_DIR}/core")
FILES_DIR = Path(f"{MAIN_DIR}/files")
DATASETS_DIR = Path(f"{MAIN_DIR}/datasets")

"""Top 10 most viwed post"""


def top_view_mapper() -> list:
    """This function reads a .xml file, searches for "post id" and "post view count" and 
    appends these values in a dictionary within a list of dictionaries.

    Returns:
        list: List of dictionaries with format {"post_id": post_id, 
                                                "view_count": view_count}. 
    """
    mytree = ET.parse(f"{DATASETS_DIR}/posts.xml")
    # Instance of the root tree.
    myroot = mytree.getroot()
    top_limit01 = len(myroot)
    post_id = None
    view_count = None
    KEY_VALUE_LIST = []
    for row in range(0, top_limit01):
        # Finding elementes.
        try:
            post_id = int(myroot[row].attrib["Id"])
            view_count = int(myroot[row].attrib["ViewCount"])
        except ValueError:
            logging.warning(
                "Warning : Parameter post_id or view_count is passed as no int value.")
            continue
        KEY_VALUE_LIST.append(
            {"post_id": post_id,
             "view_count": view_count}
        )

    # Sort list by "view_count".
    KEY_VALUE_LIST = sorted(
        KEY_VALUE_LIST, key=operator.itemgetter("view_count"), reverse=True)

    # Get top.
    POST_VIEW_COUNT_LIST = KEY_VALUE_LIST[:10]

    return POST_VIEW_COUNT_LIST


def top_view_calculator(key_value_list: list, out_file_path: str) -> object:
    """This function iter through the key_value_list and 
    find the top 10 most viewed post. 

    Args:
        key_value_list (list): Sorted ascending list of dictionaries with format: 
        {"post_id": post_id, 
         "view_count": view_count}.
        out_file_path (str): Path to output destination dir.

    Returns:
        object: Text file with top 10 most viewed posts.
    """
    # Define text file write parameters
    fl_path = out_file_path
    mode = "w"
    encoding = "utf-8"
    with open(file=fl_path, mode=mode, encoding=encoding) as file:
        post_id = None
        view_count = None
        for dictionary in key_value_list:
            # Finding elementes.
            post_id = dictionary["post_id"]
            view_count = dictionary["view_count"]
            file.write(
                f"post_id:\t{post_id}\tview_count:\t{view_count}\n")
        file.close()


"""Top 10 words by tag"""


def word_by_tag_mapper() -> list:
    """This function reads a .xml file, searches for "post tag", and "post word" and counts 
    it to append these values in a dictionary within a list of dictionaries.

    Returns:
        list: List of dictionaries sort descending by "post_tag" and "post_body_word" with format: 
        {"post_tag": post_tag, 
         "post_body_word": post_body_word, 
         "count": count}
    """
    mytree = ET.parse(f"{DATASETS_DIR}/posts.xml")
    # Instance "of" the root tree.
    myroot = mytree.getroot()
    top_limit01 = len(myroot)
    KEY_VALUE_LIST = []
    post_tag = None
    post_body_word = None
    count = None
    for row in range(0, top_limit01):
        # Finding elementes.
        try:
            # Clean XML tags from text on "Body" attribute
            text = myroot[row].attrib["Body"]
            clean_body_text = LH.fromstring(text)
            clean_body_text = clean_body_text.text_content()
            clean_body_text = re.sub(r'\n', ' ', clean_body_text)
            clean_body_text = clean_body_text.split()
            # Clean XML tags from text on "Tags" attribute.
            clean_tag_text = re.sub(r'\W+', ' ', myroot[row].attrib["Tags"])
            clean_tag_text = clean_tag_text.split()
        except KeyError:
            logging.warning(
                "Warning : Key Body or Tag doesn't exist.")
            continue
        except LH.etree.ParserError:
            continue
        # Append finded elements to dictionary within list of dictionaries.
        for tag in clean_tag_text:
            post_tag = tag
            for word in clean_body_text:
                post_body_word = word
                count = 1
                KEY_VALUE_LIST.append(
                    {"post_tag": post_tag,
                     "post_body_word": post_body_word,
                     "count": count})  # ({"post_tag": post_tag, "post_body_word": post_body_word, "count":})

    # Sort list by "post_tag" and "count".
    KEY_VALUE_LIST = sorted(
        KEY_VALUE_LIST, key=operator.itemgetter("post_tag", "post_body_word"), reverse=False)

    return KEY_VALUE_LIST


def word_by_tag_reducer(key_value_list: list) -> list:
    """This function count de number of words by tag and append these values 
    in a dictionary within a list of dictionaries..

    Args:
        key_value_list (list): List of dictionaries sort descending by "post_tag" and "post_body_word" with format: 
        {"post_tag": post_tag, 
        "post_body_word": post_body_word, 
        "count": count}

    Returns:
        list: List of dictionaries sort descending by "post_tag" and "count" with 
        format: {"post_tag": post_tag, 
                 "post_body_word": post_body_word, 
                 "count": count}
    """
    WORD_BY_CATEGORY = []
    current_word = None
    current_tag = None
    word_count = None
    # Finding elementes.
    for dictionary in key_value_list:
        tag = dictionary["post_tag"]
        word = dictionary["post_body_word"]
        count = dictionary["count"]

        if current_word == word:
            word_count += count
        else:
            if current_word:
                # Append finded elements to dictionary within list of dictionaries.
                WORD_BY_CATEGORY.append(
                    {"post_tag": current_tag,
                     "post_body_word": current_word,
                     "count": word_count})

            # Update values.
            current_word = word
            current_tag = tag
            word_count = count

    # Validate last word iteration.
    if current_word == word:
        # Append finded elements to dictionary within list of dictionaries.
        WORD_BY_CATEGORY.append(
            {"post_tag": current_tag,
             "post_body_word": current_word,
             "count": word_count})
    # Sort list by "post_tag" and "count".
    WORD_BY_CATEGORY = sorted(
        WORD_BY_CATEGORY, key=operator.itemgetter("post_tag", "count"), reverse=False)
    return WORD_BY_CATEGORY


def top_word_by_tag_calculator(key_value_list: list, out_file_path: str) -> object:
    """This function iter through a list of dictionaries and calculate
    the top 10 most repeated words by tag.

    Args:
        key_value_list (list): List of dictionaries sort descending by "post_tag" and "count" with 
        format: {"post_tag": post_tag, 
        "post_body_word": post_body_word, 
        "count": count}
        out_file_path (str): Path to output destination dir.

    Returns:
        object: Text file with top 10 most repeated words by tag.
    """
    word_by_tag = key_value_list
    top_limit = len(word_by_tag)
    current_tag = None
    aux_dict = None
    post_tag = None
    post_body_word = None
    count = None
    top_word_by_category = []

    for i in range(0, top_limit):
        # Finding elementes.
        dictionary01 = word_by_tag[i]
        next_tag = dictionary01["post_tag"]

        # Validate tag iteration.
        if current_tag == next_tag:
            pass
        else:
            if current_tag:
                # Start countdown since current list index (dictionary) to append the
                # top 10 most repeated words for the set tag.
                counter = 1
                while counter <= 10:
                    to_dict_indx = (i - counter)
                    aux_dict = word_by_tag[to_dict_indx]
                    top_word_by_category.append(aux_dict)
                    counter += 1

            current_tag = next_tag
    # Validate last tag iteration.
    if current_tag == next_tag:
        counter = 1
        # Start countdown since current list index (dictionary) to append the
        # top 10 most repeated words for the set tag.
        while counter <= 10:
            to_dict_indx = (i - counter)
            aux_dict = word_by_tag[to_dict_indx]
            top_word_by_category.append(aux_dict)
            counter += 1

    # Define text file write parameters
    fl_path = out_file_path
    mode = "w"
    encoding = "utf-8"

    with open(file=fl_path, mode=mode, encoding=encoding) as file:
        for dictionary in top_word_by_category:
            post_tag = dictionary["post_tag"]
            post_body_word = dictionary["post_body_word"]
            count = dictionary["count"]
            file.write(
                f"post_tag:\t{post_tag}\tpost_body_word:\t{post_body_word}\tcount:\t{count}\n")
        file.close()


"""Response time"""


def datetime_obj(datetime_str: str) -> datetime.date:
    # Convert to datetime object
    some_datetime_obj = dateutil.parser.parse(datetime_str)

    # Get Year, Month, and Day as integers
    year = some_datetime_obj.year
    month = some_datetime_obj.month
    day = some_datetime_obj.day
    hour = some_datetime_obj.hour
    min = some_datetime_obj.minute
    sec = some_datetime_obj.second

    # Define dateformat
    date_format = "{year}/{month:02d}/{day:02d} {hour}:{min}:{sec}"
    date_string = date_format.format(year=year, month=month,
                                     day=day, hour=hour,
                                     min=min, sec=sec)

    # Caculate date
    format = "%Y/%m/%d %H:%M:%S"
    date = datetime.datetime.strptime(date_string, format)

    return date


def response_time_mapper() -> list:
    """This function reads a .xml file, searches for "post_id", "post_score", "post_type", "parent_id",
    "accepted_answer_id" and"creation_date" to appends these values in a dictionary within a list of dictionaries.

    Returns:
        list: List of dictionaries sort descending by "post_type" and "score" with
        format: {"id": id, 
                 "score": score, 
                 "post_type": post_type, 
                 "parent_id": parent_id, 
                 "accepted_answer_id": accepted_answer_id, 
                 "creation_date": creation_date}
    """
    mytree = ET.parse(f"{DATASETS_DIR}/posts.xml")
    # Instance "of" the root tree
    myroot = mytree.getroot()
    KEY_VALUE_LIST = []
    min_rank = 199
    max_rank = 299
    id = None
    score = None
    post_type = None
    parent_id = None
    accepted_answer_id = None
    creation_date = None

    for row in range(min_rank, max_rank):
        # Finding elementes.
        id = int(myroot[row].attrib["Id"])
        score = int(myroot[row].attrib["Score"])
        post_type = int(myroot[row].attrib["PostTypeId"])
        creation_date = (myroot[row].attrib["CreationDate"])
        creation_date = datetime_obj(creation_date)

        try:
            accepted_answer_id = int(myroot[row].attrib["AcceptedAnswerId"])
        except KeyError:
            logging.warning(
                "Warning : Key AcceptedAnswerId doesn't exist.")
            accepted_answer_id = None

        try:
            parent_id = int(myroot[row].attrib["ParentId"])
        except KeyError:
            logging.warning(
                "Warning : Key ParentId doesn't exist.")
            parent_id = None

        # Append finded elements to dictionary within list of dictionaries.
        KEY_VALUE_LIST.append({"id": id, "score": score, "post_type": post_type, "parent_id": parent_id,
                               "accepted_answer_id": accepted_answer_id, "creation_date": creation_date})

    # Sort list by "post_type", "score".
    KEY_VALUE_LIST = sorted(
        KEY_VALUE_LIST, key=operator.itemgetter("post_type", "score"), reverse=False)

    return KEY_VALUE_LIST


def response_time_mean_calculator(key_value_list: list) -> int:
    """This function iter throuht key_value_list and .xml file to calculate the response time mean value (sec)
    betwent the posts and their respective answer.

    Args:
        key_value_list (list): List of dictionaries sort descending by "post_type" and "score" with
        format: {"id": id, 
                 "score": score, 
                 "post_type": post_type, 
                 "parent_id": parent_id, 
                 "accepted_answer_id": accepted_answer_id, 
                 "creation_date": creation_date}

    Returns:
        int: Response time mean value (sec)
    """
    mytree = ET.parse(f"{DATASETS_DIR}/posts.xml")
    # Instance "of" the root tree
    myroot = mytree.getroot()
    top_limit01 = len(myroot)
    match_value = None
    post_id = None
    answer_id = None
    parent_id = None
    post_creation_date = None
    response_time = None
    RESPONSE_TIME_LIST = []
    mean = None
    MEAN = []

    for dictionary in key_value_list:

        # Find elementes.
        post_id = dictionary["id"]
        # Validate post type: question
        if dictionary["post_type"] == 1:
            # Find the creation date and respective answer.
            post_creation_date = dictionary["creation_date"]
            answer_id = dictionary["accepted_answer_id"]
            # If the answer exists then validate the match between the post (question)
            # in the list of dictionaries and the .xml file id attribute (match value) through answer_id.
            if answer_id:
                for row in range(0, top_limit01):
                    match_value = int(myroot[row].attrib["Id"])
                    # If there is match append the find values to the list of dictionaries
                    if match_value == answer_id:
                        answer_creation_date = (
                            myroot[row].attrib["CreationDate"])
                        # Transform datetime xml string to datetime object
                        answer_creation_date = datetime_obj(
                            answer_creation_date)
                        # Calculate the difference between answer_creation_date and post_creation_date
                        # (response time) and return a time delta object. It's necessary to convert it to seconds and then to integer.
                        response_time = (answer_creation_date -
                                         post_creation_date).seconds
                        response_time = int(response_time)
                        # Append the find values to the list of dictionaries.
                        RESPONSE_TIME_LIST.append(
                            {"post_id": post_id,
                             "post_creation_date": post_creation_date,
                             "answer_id": answer_id,
                             "answer_creation_date": answer_creation_date,
                             "response_time(seg)": response_time}
                        )
                        # Append the response_time to the MEAN list.
                        MEAN.append(response_time)
                        break
            else:
                # If there is no answer append the None values to the list of dictionaries
                wth_answer = None
                RESPONSE_TIME_LIST.append(
                    {"post_id": post_id,
                     "post_creation_date": post_creation_date,
                     "answer_id": wth_answer,
                     "answer_creation_date": wth_answer,
                     "response_time": wth_answer}
                )
        # Validate post type: answer
        elif dictionary["post_type"] == 2:
            parent_id = dictionary["parent_id"]
            answer_creation_date = dictionary["creation_date"]
            for row in range(0, top_limit01):
                match_value = int(myroot[row].attrib["Id"])
                # Validate the match between the post (answer)
                # in the list of dictionaries and the .xml file id attribute (match value) through parent_id.
                # If there is match append the find values to the list of dictionaries
                if match_value == parent_id:
                    post_creation_date = (myroot[row].attrib["CreationDate"])
                    post_creation_date = datetime_obj(post_creation_date)
                    # Calculate the difference between answer_creation_date and post_creation_date
                    # (response time) and return a time delta object. It's necessary to convert it to seconds and then to integer.
                    response_time = (answer_creation_date -
                                     post_creation_date).seconds
                    response_time = int(response_time)
                    RESPONSE_TIME_LIST.append(
                        {"post_id": post_id,
                         "post_creation_date": post_creation_date,
                         "answer_id": answer_id,
                         "answer_creation_date": answer_creation_date,
                         "response_time(seg)": response_time}
                    )
                    # Append the response_time to the MEAN list.
                    MEAN.append(response_time)
                    break

    # Calculate response time mean value.
    mean = round(sts.mean(MEAN), 2)
    return mean
