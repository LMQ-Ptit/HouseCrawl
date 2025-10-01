from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
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

def crawl_batdongsan(page=1):
    """Cào dữ liệu từ batdongsan.com.vn"""
    driver = setup_driver()
    product_links = []
    
    try:
        # 1. Truy cập website
        url = "https://batdongsan.com.vn/nha-dat-ban-ha-noi/p" + str(page)
        print(f"Đang truy cập {url}...")
        driver.get(url)
        
        # Đợi trang tải xong
        time.sleep(5)
        
        # 2. Tìm container sản phẩm
        print("Đang tìm container sản phẩm...")
        product_container = driver.find_element(By.ID, "product-lists-web")
        
        # Cuộn trang để đảm bảo tải tất cả sản phẩm
        driver.execute_script("arguments[0].scrollIntoView(true);", product_container)
        time.sleep(2)
        
        # 3. Tìm các thẻ div con có class chứa "js__card js__card-full-web"
        print("Đang tìm các sản phẩm...")
        product_cards = product_container.find_elements(By.CSS_SELECTOR, 
            "div.js__card.js__card-full-web.pr-container.re__card-full.re__vip-diamond")
        
        print(f"Đã tìm thấy {len(product_cards)} sản phẩm. Đang trích xuất liên kết...")
        
        # 4. Chỉ lấy link từ các sản phẩm
        for i, card in enumerate(product_cards, 1):
            try:
                link = card.find_element(By.TAG_NAME, "a").get_attribute("href")
                product_links.append(link)
                print(f"Đã lấy link [{i}/{len(product_cards)}]: {link}")
            except Exception as e:
                print(f"Không thể lấy link cho sản phẩm {i}: {e}")
        
        # 5. Lưu tất cả link vào file JSON (ĐÃ CHỈNH SỬA - CỘNG DỒN THAY VÌ GHI ĐÈ)
        # Kiểm tra xem file đã tồn tại chưa
        import os
        existing_links = []
        if os.path.exists('linkProduct.json') and os.path.getsize('linkProduct.json') > 0:
            try:
                with open('linkProduct.json', 'r', encoding='utf-8') as f:
                    existing_links = json.load(f)
                print(f"Đã tìm thấy file cũ với {len(existing_links)} liên kết")
            except json.JSONDecodeError:
                print("File JSON hiện tại không hợp lệ, tạo mới")
                existing_links = []
        
        # Thêm các liên kết mới vào danh sách hiện có
        for link in product_links:
            if link not in existing_links:
                existing_links.append(link)
                
        # Ghi toàn bộ danh sách đã cập nhật vào file
        with open('linkProduct.json', 'w', encoding='utf-8') as f:
            json.dump(existing_links, f, ensure_ascii=False, indent=4)
        
        print(f"\nHoàn thành! Đã lưu {len(existing_links)} liên kết vào file linkProduct.json")
        print(f"(Trong đó có {len(product_links)} liên kết mới từ lần cào này)")
        
    except Exception as e:
        print(f"Lỗi: {e}")
    
    finally:
        # Đóng trình duyệt
        print("Đóng trình duyệt...")
        driver.quit()
        
        return product_links
    """Cào dữ liệu từ batdongsan.com.vn"""
    driver = setup_driver()
    product_links = []
    
    try:
        # 1. Truy cập website
        url = "https://batdongsan.com.vn/nha-dat-ban-ha-noi/p3"
        print(f"Đang truy cập {url}...")
        driver.get(url)
        
        # Đợi trang tải xong
        time.sleep(5)
        
        # 2. Tìm container sản phẩm
        print("Đang tìm container sản phẩm...")
        product_container = driver.find_element(By.ID, "product-lists-web")
        
        # Cuộn trang để đảm bảo tải tất cả sản phẩm
        driver.execute_script("arguments[0].scrollIntoView(true);", product_container)
        time.sleep(2)
        
        # 3. Tìm các thẻ div con có class chứa "js__card js__card-full-web"
        print("Đang tìm các sản phẩm...")
        product_cards = product_container.find_elements(By.CSS_SELECTOR, 
            "div.js__card.js__card-full-web.pr-container.re__card-full.re__vip-diamond")
        
        print(f"Đã tìm thấy {len(product_cards)} sản phẩm. Đang trích xuất liên kết...")
        
        # 4. Chỉ lấy link từ các sản phẩm
        for i, card in enumerate(product_cards, 1):
            try:
                link = card.find_element(By.TAG_NAME, "a").get_attribute("href")
                product_links.append(link)
                print(f"Đã lấy link [{i}/{len(product_cards)}]: {link}")
            except Exception as e:
                print(f"Không thể lấy link cho sản phẩm {i}: {e}")
        
        # 5. Lưu tất cả link vào file JSON
        with open('linkProduct.json', 'w', encoding='utf-8') as f:
            json.dump(product_links, f, ensure_ascii=False, indent=4)
        
        print(f"\nHoàn thành! Đã lưu {len(product_links)} liên kết vào file linkProduct.json")
        
    except Exception as e:
        print(f"Lỗi: {e}")
    
    finally:
        # Đóng trình duyệt
        print("Đóng trình duyệt...")
        driver.quit()
        
        return product_links

if __name__ == "__main__":
    for i in range(1, 2890): 
        crawl_batdongsan(page=i)