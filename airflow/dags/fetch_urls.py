from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def fetch_cms_urls():
    remote_url = "http://selenium_chrome:4444/wd/hub"  
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--incognito")
    
    driver = webdriver.Remote(command_executor=remote_url, options=options)
    
    url = "https://data.cms.gov/collection/synthetic-medicare-enrollment-fee-for-service-claims-and-prescription-drug-event"
    driver.get(url)
    
    try:
        sections = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "RichTextField.ArticleTextBlock__body"))
        )
    except Exception as e:
        print("Sections not found!", e)
        driver.quit()
        return [], []
    
    if len(sections) < 3:
        print("Not enough sections to exclude the first and last!")
        driver.quit()
        return [], []
    
    cms_section = sections[1:-1]
    excluded_keywords = {"All FFS Claims", "All Beneficiary Years"}
    
    claims_urls = []
    beneficiary_urls = []
    
    for section in cms_section:
        links = section.find_elements(By.TAG_NAME, "a")
        for link in links:
            href = link.get_attribute("href")
            text = link.text.strip()
            if text in excluded_keywords:
                continue
            if "Beneficiary" in text:
                beneficiary_urls.append(href)
            else:
                claims_urls.append(href)
    
    driver.quit()
    return claims_urls, beneficiary_urls


# if __name__ == "__main__":
#     claims, beneficiaries = fetch_cms_urls()
#     print("Claims URLs:", claims)
#     print("Beneficiary URLs:", beneficiaries)
