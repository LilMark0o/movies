from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time


def getImage(query):
    driver = webdriver.Chrome()

    try:
        driver.get("https://www.google.com/imghp")

        search_box = driver.find_element(By.NAME, "q")

        search_box.send_keys(query + Keys.RETURN)

        time.sleep(2)

        first_image = driver.find_element(
            By.CSS_SELECTOR, '#islrg > div.islrc > div:nth-child(2) > a.FRuiCf.islib.nfEiy > div.fR600b.islir > img')

        image_url = None
        if first_image:
            image_url = first_image.get_attribute("src")

        time.sleep(2)

    finally:
        # Close the WebDriver
        driver.quit()

    if image_url == None:
        return "./assets/movie.png"
    else:
        return image_url
