from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import pandas as pd

def setup_driver():
    """Khởi tạo trình duyệt Chrome"""
    options = Options()
    # options.add_argument("--headless")  # Bỏ comment nếu muốn chạy ẩn
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    # User-Agent giống trình duyệt thật
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    # Giả mạo thông tin webdriver
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def crawl_property_info(url):
    """Cào thông tin bất động sản từ URL cụ thể"""
    driver = setup_driver()
    try:
        print(f"Đang truy cập URL: {url}")
        driver.get(url)
        
        # Đợi trang tải xong
        time.sleep(2)
        
        # Tìm tất cả các thẻ div có class="re__pr-specs-content-item"
        spec_items = driver.find_elements(By.CSS_SELECTOR, "div.re__pr-specs-content-item")
        
        print(f"\nĐã tìm thấy {len(spec_items)} thẻ div có class='re__pr-specs-content-item':")
        for i, item in enumerate(spec_items, 1):
            print(f"\n--- Item {i} ---")
            print(item.text)
            
        # Tìm thẻ div đầu tiên có class="re__pr-short-info-item js__pr-config-item"
        try:
            first_short_info = driver.find_elements(By.CSS_SELECTOR, "div.re__pr-short-info-item.js__pr-config-item")
            print("\n\n2 Thẻ div đầu tiên có class='re__pr-short-info-item js__pr-config-item':")
            for item in first_short_info[:2]:
                print(item.text)
        except Exception as e:
            print(f"\nKhông tìm thấy thẻ div có class='re__pr-short-info-item js__pr-config-item': {e}")
            
    except Exception as e:
        print(f"Lỗi khi cào dữ liệu: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    url = "https://batdongsan.com.vn/ban-can-ho-chung-cu-phuong-me-tri-prj-the-matrix-one-premium/chuyen-chuyen-nhuong-quy-2pn-3pn-4pn-tai-quy-thang-10-2025-hot-pr44163169"
    crawl_property_info(url)