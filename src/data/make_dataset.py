
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from newspaper import Article
import pandas as pd
import time
import requests


def get_articles(driver, link):
    driver.get(link)
    
    links_article = []
    wait = WebDriverWait(driver, 100)  
    i = 0
    while i < 40:
        try:
            
            xem_them_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'list__viewmore') and contains(@class, 'list__center') and contains(@class, 'view_more') and @title='Xem thêm']")))
            xem_them_button.click()
            print('xem them')
            i += 1
            time.sleep(5) 
        except Exception as e:
            continue
            
    news_links = driver.find_elements(By.CSS_SELECTOR, '.item a')  
    for l in news_links:
        href = l.get_attribute('href')
        print(href)
        if href:
            if href[8] == 'm':
                url = href[:8] + href[10:]
                if url not in links_article:
                    links_article.append(url)
            else:
                if href not in links_article:
                    links_article.append(href)

    
    return links_article

def download_article(links_article, path_file):
    temp_df = {
        'Title': [],
        'Text': [],
        'Key Word': [],
        'Published_Date': [],
        'Source': []
    }
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

    for link in links_article:
        success = False
        attempts = 3 
        for attempt in range(attempts):
            try:
                response = requests.get(link, headers=headers, timeout=10)
                response.raise_for_status() 

                article = Article(link)
                article.set_html(response.text)
                article.parse()
                article.nlp()

                temp_df['Title'].append(article.title if article.title else '')
                temp_df['Text'].append(article.text if article.text else '')
                temp_df['Key Word'].append(', '.join(article.keywords) if article.keywords else '')
                temp_df['Published_Date'].append(article.publish_date if article.publish_date else '')
                temp_df['Source'].append(article.source_url if article.source_url else link)

                success = True
                break

            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt + 1}/{attempts} failed for article {link}: {e}")
                time.sleep(5)

        if not success:
            print(f"Failed to process article {link} after {attempts} attempts.")
            
    final_df = pd.DataFrame(temp_df)
    final_df.to_csv(path_file, index=False)


if __name__ == '__main__':
    links_menu = {
        'thi_truong_ck' : 'https://m.cafef.vn/thi-truong-chung-khoan.chn', 
        'doanh_nghiep': 'https://cafef.vn/doanh-nghiep.chn', 
        'tai_chinh_ngan_hang': 'https://m.cafef.vn/tai-chinh-ngan-hang.chn', 
        'tai_chinh_quoc_te': 'https://m.cafef.vn/tai-chinh-quoc-te.chn',
        'kinh_te_vi_mo': 'https://cafef.vn/vi-mo-dau-tu.chn',
        'kinh_te_so': 'https://m.cafef.vn/kinh-te-so.chn',
        'thi_truong': 'https://m.cafef.vn/thi-truong.chn',
        'goc_nhin_chuyen_gia': 'https://m.cafef.vn/bai-viet-cua-cac-chuyen-gia.chn'
    }

    mobile_emulation = {
        "userAgent": ("Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) "
                    "AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 "
                    "Mobile/15A372 Safari/604.1")
    }

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("mobileEmulation", mobile_emulation)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    for link in links_menu.keys():
        links_art = get_articles(driver, links_menu[link])
        print(len(links_art))
        path_file = f'data\\raw\\{link}.csv'
        download_article(links_art, path_file)

    driver.quit()
    

