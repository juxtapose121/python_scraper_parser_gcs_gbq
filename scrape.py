import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains

options = Options()
options.add_argument('--headless')
options.add_argument('--window-size=1920,1080')

driver = webdriver.Chrome("/usr/bin/chromedriver", options=options)


driver.get("https://www.iemop.ph/market-data/rtd-reserve-schedules/")

time.sleep(2)
download = driver.find_element_by_xpath(
    '//*[@id="list-cont"]/div[1]/div/a/button')

ActionChains(driver).click(download).perform()
