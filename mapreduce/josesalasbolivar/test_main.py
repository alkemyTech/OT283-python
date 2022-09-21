import modules.map_reduce_function as mp_funct
import random
import pytest
#from os import path

"""Top 10 most viewed post"""


def test_top_view_calculator(tmpdir):
    """This function compares a key_values list of dictionaries with the format: 
        {"post_id": post_id, 
         "view_count": view_count}
         and a .txt file writed through the list to validate its format and structure.
    """
    keys_list = []
    # Define the path to temporary dir.
    out_file_path = f"{tmpdir}/test_output_top_view_count.txt"
    # Instance key value list as input to write de .txt file.
    data_in = mp_funct.top_view_mapper()
    mp_funct.top_view_calculator(
        key_value_list=data_in, out_file_path=out_file_path)
    top_limit = len(data_in)
    dict_index = random.randint(0, top_limit)
    dictionary = data_in[dict_index]
    keys_list = list(dictionary.keys())
    # Instantiate key values to compare.
    data_in_key01 = keys_list[0]
    data_in_key02 = keys_list[1]

    # Define text file read parameters.
    mode = "r"
    encoding = "utf-8"
    with open(file=out_file_path, mode=mode, encoding=encoding) as file_out:
        for line in file_out:
            line = line.split()
            # Instantiate key values to compare.
            data_out_key01 = line[0].replace(':', '')
            data_out_key02 = line[2].replace(':', '')
            break
        file_out.close()

    # Compare key values list items with .txt file items.
    assert data_in_key01 == data_out_key01 and data_in_key02 == data_out_key02


"""Top 10 words by tag"""


def test_word_by_tag_reducer(tmpdir):
    """This function compares a key_values list of dictionaries with the format: 
        {"post_tag": post_tag, 
         "post_body_word": post_body_word, 
         "count": count}
         and a .txt file writed through the list to validate its format and structure.
    """
    keys_list = []
    # Define the path to temporary dir.
    out_file_path = f"{tmpdir}/output_top_words_by_tag.txt"
    # Instance key value list as input to write de .txt file.
    test_word_by_tag = mp_funct.word_by_tag_mapper()
    data_in = mp_funct.word_by_tag_reducer(
        key_value_list=test_word_by_tag)
    mp_funct.top_word_by_tag_calculator(
        key_value_list=data_in, out_file_path=out_file_path)
    top_limit = len(data_in)
    dict_index = random.randint(0, top_limit)
    dictionary = data_in[dict_index]
    keys_list = list(dictionary.keys())
    # Instantiate key values to compare.
    data_in_key01 = keys_list[0]
    data_in_key02 = keys_list[1]
    data_in_key03 = keys_list[2]

    # Define text file read parameters.
    mode = "r"
    encoding = "utf-8"
    with open(file=out_file_path, mode=mode, encoding=encoding) as file_out:
        for line in file_out:
            line = line.split()
            # Instantiate key values to compare.
            data_out_key01 = line[0].replace(':', '')
            data_out_key02 = line[2].replace(':', '')
            data_out_key03 = line[4].replace(':', '')
            break
        file_out.close()
    # Compare key values list items with .txt file items.
    assert data_in_key01 == data_out_key01 and data_in_key02 == data_out_key02 and data_in_key03 == data_out_key03


"""Response Time"""


def test_response_time():
    """This function validates the real response time 
    mean value with the calculated response time mean value 
    """
    # Sort the post by post type.
    test_key_value_list = mp_funct.response_time_mapper()
    # Calculate the response time mean value.
    test_response_time_mean_value = mp_funct.response_time_mean_calculator(
        key_value_list=test_key_value_list)
    response_time_mean_value = 8195.38
    assert response_time_mean_value == test_response_time_mean_value
