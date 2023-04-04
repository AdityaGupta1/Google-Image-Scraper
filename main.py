# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 11:02:06 2020

@author: OHyic

"""
#Import libraries
import os
import concurrent.futures
from GoogleImageScraper import GoogleImageScraper
from patch import webdriver_executable
from threading import Lock
import pandas
import time
import sys

images_dict = {}
images_dict_lock = Lock()

in_df = None


def worker_thread(search_key):
    image_scraper = GoogleImageScraper(
        webdriver_path, image_path, in_df.loc[search_key][1], number_of_images, headless, min_resolution, max_resolution)
    image_urls = image_scraper.find_image_urls()

    with images_dict_lock:
        images_dict[search_key] = image_urls[0];

    #Release resources
    del image_scraper


def print_time(message, start_time, num_entities):
    print()
    print('=============================================')
    print(message)
    print('-----------------------------------')
    time_elapsed = round(time.time() - start_time, 3)
    print('seconds elapsed: ' + str(time_elapsed))
    print('entities per second: ' + str(round(num_entities / time_elapsed, 3)))
    print('=============================================')


if __name__ == "__main__":
    file_num_min = int(sys.argv[1])
    file_num_max = file_num_min if len(sys.argv) < 3 else int(sys.argv[2])
    num_rows_per_file = 1000
    
    for file_num in range(file_num_min, file_num_max + 1):
        images_dict.clear()

        skip = range((num_rows_per_file) * (file_num - 1) + 1) if file_num != 1 else None

        in_csv_loc = os.path.normpath(os.path.join(os.getcwd(), 'in/entities.csv'))
        in_df = pandas.read_csv(in_csv_loc, skiprows=skip, nrows=num_rows_per_file, index_col=0, header=None)
        in_df.index.rename('entity_id', inplace=True)

        start_time = time.time()

        #Define file path
        webdriver_path = os.path.normpath(os.path.join(os.getcwd(), 'webdriver', webdriver_executable()))
        image_path = os.path.normpath(os.path.join(os.getcwd(), 'photos'))

        search_keys = in_df.index.tolist()

        #Parameters
        number_of_images = 1                # Desired number of images
        headless = True                     # True = No Chrome GUI
        min_resolution = (0, 0)             # Minimum desired image resolution
        max_resolution = (9999, 9999)       # Maximum desired image resolution
        max_missed = 1000                   # Max number of failed images before exit
        number_of_workers = 12              # Number of "workers" used
        keep_filenames = False              # Keep original URL image filenames

        #Run each search_key in a separate thread
        #Automatically waits for all threads to finish
        #Removes duplicate strings from search_keys
        with concurrent.futures.ThreadPoolExecutor(max_workers=number_of_workers) as executor:
            executor.map(worker_thread, search_keys)

        print_time('DONE SCRAPING FILE ' + str(file_num), start_time, num_rows_per_file)
        print()

        out_df = pandas.DataFrame.from_dict(images_dict, orient='index')
        out_df.to_csv(os.path.normpath(os.path.join(os.getcwd(), 'out/urls_' + str(file_num) + '.csv')), index=True, header=False)

        print('DONE WRITING FILE ' + str(file_num))