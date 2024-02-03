from selenium.webdriver.chrome.options import Options
import selenium.webdriver.remote.webdriver
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import selenium


def get_driver():
    chrome_options = Options()
    # chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    # Change chrome driver path accordingly
    # chrome_driver = "C:\chromedriver.exe"
    driver = webdriver.Chrome(executable_path='/home/jyotiraditya/Downloads/chromedriver-linux64/chromedriver',chrome_options=chrome_options)
    driver.maximize_window()
    return driver

def get_debugger_driver(controller):
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    # Change chrome driver path accordingly
    # chrome_driver = "C:\chromedriver.exe"
    controller.driver = webdriver.Chrome(executable_path='/home/jyotiraditya/Desktop/chromedriver-linux64/chromedriver',chrome_options=chrome_options)
    print(controller.driver.title)
    controller.driver.maximize_window()



def scroll_to_element(driver, element):
    driver.execute_script("arguments[0].scrollIntoView();", element)


def scroll_to_top(driver):
    driver.execute_script('window.scrollTo(0, 0);')


def click_(driver, element):
    driver.execute_script("arguments[0].click();", element)


def get_element(driver, by, locator, parent=None, timeout=3) -> selenium.webdriver.remote.webdriver.WebElement:
    parent = parent if parent is not None else driver
    try:
        element = WebDriverWait(parent, timeout).until(
            EC.presence_of_element_located((by, locator))
        )
        scroll_to_element(driver, element)
        return element
    except Exception as e:
        return None


def get_all_elements(driver, by, locator, parent=None, timeout=3):
    parent = parent if parent is not None else driver
    try:
        elements = WebDriverWait(parent, timeout).until(
            EC.presence_of_all_elements_located((by, locator))
        )
        return elements
    except Exception as e:
        print(e)
        return None


def click_element(driver, element, parent=None, timeout=3):
    scroll_to_element(driver, element)
    parent = parent if parent is not None else driver
    try:
        element = WebDriverWait(parent, timeout).until(
            EC.element_to_be_clickable(element)
        )
        return element
    except Exception as e:
        return None


def wait_for_element_to_load(driver, element, timeout=3):
    scroll_to_element(driver, element)
    try:
        WebDriverWait(driver, timeout).until(
            EC.visibility_of(element)
        )
        return 1
    except Exception as e:
        return 0


if __name__ == "__main__":
    class a:
        def __init__(self):
            pass


    get_driver(a())
