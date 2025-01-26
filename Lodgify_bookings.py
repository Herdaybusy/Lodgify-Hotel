import selenium
print(selenium.__version__)
import pandas as pd
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
import requests
from dotenv import load_dotenv
import os 
import re
from selenium import webdriver 
from selenium.webdriver.chrome.service import Service 
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time  

# Data Extraction
options = Options()
options.headless = True

service = Service(ChromeDriverManager().install())

driver = webdriver.Chrome(service=service, options=options)
url = 'https://www.booking.com/searchresults.html?ss=london&efdco=1&label=gen173nr-1FCAEoggI46AdIM1gEaFCIAQGYATG4ARfIAQzYAQHoAQH4AQKIAgGoAgO4AqqI1LwGwAIB0gIkNjkyMzM1ZDEtNzUzZC00YzlmLWFiZTYtMWZiNjE4ZWUwZTAz2AIF4AIB&aid=304142&lang=en-us&sb=1&src_elem=sb&src=index&dest_id=-2601889&dest_type=city&place_id=city%2F-2601889&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=8fa56d55e5270165&ac_meta=GhA1NmEwNmQ2YjQ3OGQwM2U2IAAoATICZW46BmxvbmRvbkAASgBQAA%3D%3D&checkin=2025-01-25&checkout=2025-02-28&group_adults=2&no_rooms=1&group_children=0'

driver.get(url)

time.sleep(30)

page_source = driver.page_source


scroll_time = 10
last_height = driver.execute_script('return document.body.scrollHeight')
while True:
    # scroll down to the bottom
    driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')

    time.sleep(scroll_time)
    # new height
    new_height = driver.execute_script('return document.body.scrollHeight')
    
    if new_height == last_height:
        break
    
    last_height = new_height
    
    
page_source = driver.page_source
print('successful')
driver.quit()

soup = BeautifulSoup(page_source, 'html.parser')

hotels =soup.find_all('div', {'data-testid' : 'property-card'}

hotels_data = []
for hotel in hotels:
    name_element = hotel.find('div', {'data-testid' : 'title'})
    name = name_element.text.strip() if name_element else 'N/A'
    
    link_element = hotel.find('a', {'data-testid' : 'title-link'})
    link = link_element['href'] if link_element else 'N/A'
    
    location_element =hotel.find('span', {'data-testid' : 'address'})
    location = location_element.text.strip() if location_element else 'N/A'
    
    price_element =hotel.find('span', {'data-testid' : 'price-and-discounted-price'})
    price = price_element.text.strip() if price_element else 'N/A'
    
    rating_element = hotel.find('div', {'data-testid' : 'review-score'})
    if rating_element:
        rating = rating_element.text.strip().split()[1]
    else: 
        rating = 'N/A'
    
    hotels_data.append({
        'name' : name,
        'link' : link,
        'location' : location,
        'price' : price,
        'rating' :rating 
         
    })
final_data = pd.DataFrame(hotels_data)

# Transformation

def clean_price(price):
    return price.replace('£', '').replace(',', '').strip()
final_data['price'] =  final_data['price'].apply(clean_price)

# load to database
load_dotenv(override=True)
db_user = os.getenv('DB_USER')
db_name = os.getenv('DB_NAME')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_password = os.getenv('DB_PASSWORD')

database_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(database_url)
display('Connected successfully')

final_data.to_sql('Hotel_data',engine, if_exists='replace', index=False)