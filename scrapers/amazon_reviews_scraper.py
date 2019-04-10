# amazon_reviews_scraper.py
# The following code was partially adapated from Abe Flansburg (https://github.com/aflansburg)

from helpers import read_reviews
from helpers import is_valid_file
import os
import csv
import io


inputFile = '/scraped/'
#print(inputFile)

# Chrome driver
if os.name == 'posix':
    driver_path = '/usr/bin/chromedriver'
else:
    print('Unknown operating system!!!')
    exit()

data = read_reviews(driver_path, inputFile)

field_names = ['asin', 'product_title', 'rating', 'review_title', 'variation', 'review_text', 'review-links', 'review-date', 'verified', 'category']

expanded_reviews = []

for product_reviews in data:
    _asin = product_reviews['asin']
    _title = product_reviews['title']
    _data = product_reviews['data']

    for _d in _data:
        expanded_reviews.append([_asin, _title, _d[0], _d[1], _d[2], _d[3], _d[4], _d[5], _d[6], _d[7]])

with io.open('reviews.csv', 'w', encoding="utf-8", newline='') as dataFile:
    writer = csv.writer(dataFile, delimiter=',')

    writer.writerow(field_names)
    for e in expanded_reviews:
        writer.writerow(e)

    print(f'Output written to "reviews.csv"')




