from bs4 import BeautifulSoup as BS
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import csv
import math
import re
import pprint
import os.path

import argparse
import os
import io

#pp = pprint.PrettyPrinter(indent=4)



def read_products_csv(fn):
    links_list = []
    with open(fn, newline='') as csvfile:
        asin_reader = csv.reader(csvfile, delimiter=',')
        for row in asin_reader:
            links_list.append(row[2])
    return links_list

def get_asin(file):
    links = read_products_csv(file)
    asin = []

    if len(links)>0:
        for link in links:
            if("amazon" in link):
                asin.append((link.rsplit('/dp/', 1)[1]).split('?')[0])
                #asin.append(link.split("/dp/ ?"))
    return asin
 



if __name__ == "__main__":
    inputFile = "1.csv"
    asin_list = get_asin(inputFile)

    with open("asins.csv", 'w', newline='') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        for i in asin_list:
            wr.writerow([i])
        
    
    #print(links)
    #print(len(links))