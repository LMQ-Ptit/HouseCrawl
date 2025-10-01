from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import os
from tqdm import tqdm

def setup_driver():
    """Khởi tạo trình duyệt Chrome"""
    options = Options()
    options.add_argument("--headless")  # Chạy ẩn để tăng tốc độ
    options.add_argument("--no-sandbox")
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

def save_progress(links, last_page):
    """Lưu tiến độ hiện tại để có thể tiếp tục sau khi dừng"""
    progress_data = {
        'last_completed_page': last_page,
        'total_links': len(links)
    }
    
    with open('progress.json', 'w', encoding='utf-8') as f:
        json.dump(progress_data, f, ensure_ascii=False, indent=4)
    
    # Lưu các liên kết đã thu thập được
    with open('linkProduct.json', 'w', encoding='utf-8') as f:
        json.dump(links, f, ensure_ascii=False, indent=4)
    
    print(f"\nĐã lưu tiến độ: trang {last_page}, tổng {len(links)} liên kết")

def load_progress():
    """Tải tiến độ đã lưu trước đó (nếu có)"""
    if os.path.exists('progress.json'):
        try:
            with open('progress.json', 'r', encoding='utf-8') as f:
                progress = json.load(f)
                last_page = progress.get('last_completed_page', 0)
            
            # Tải các liên kết đã thu thập trước đó
            if os.path.exists('linkProduct.json'):
                with open('linkProduct.json', 'r', encoding='utf-8') as f:
                    links = json.load(f)
                return last_page, links
        except Exception as e:
            print(f"Lỗi khi tải tiến độ: {e}")
    
    return 0, []  # Trả về trang 0 và danh sách rỗng nếu không có tiến độ

def crawl_page(driver, page_number):
    """Cào dữ liệu từ một trang cụ thể"""
    page_links = []
    
    # Tạo URL với số trang
    url = f"https://batdongsan.com.vn/nha-dat-ban-ha-noi/p{page_number}"
    
    try:
        # Truy cập trang
        driver.get(url)
        
        # Đợi trang tải xong
        time.sleep(5)
        
        # Tìm container sản phẩm
        product_container = driver.find_element(By.ID, "product-lists-web")
        
        # Cuộn trang để đảm bảo tải tất cả sản phẩm
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/3);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, 2*document.body.scrollHeight/3);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        
        # Tìm các thẻ div sản phẩm
        product_cards = product_container.find_elements(By.CSS_SELECTOR, 
            "div[class*='js__card']")  # Tìm tất cả các loại sản phẩm, không chỉ VIP Diamond
        
        # Lấy link từ các sản phẩm
        for card in product_cards:
            try:
                link = card.find_element(By.TAG_NAME, "a").get_attribute("href")
                if link and link not in page_links:  # Tránh trùng lặp
                    page_links.append(link)
            except Exception as e:
                continue  # Bỏ qua nếu không lấy được link
        
        print(f"Trang {page_number}: tìm thấy {len(page_links)} liên kết")
        
    except Exception as e:
        print(f"Lỗi khi xử lý trang {page_number}: {e}")
    
    return page_links

def crawl_batdongsan(start_page=1, end_page=2895):
    """Cào dữ liệu từ nhiều trang"""
    # Tải tiến độ trước đó nếu có
    last_completed_page, all_links = load_progress()
    
    if last_completed_page > 0:
        print(f"Tiếp tục từ trang {last_completed_page + 1}, đã có {len(all_links)} liên kết")
        start_page = last_completed_page + 1
    else:
        all_links = []
    
    # Kiểm tra xem có cần tiếp tục không
    if start_page > end_page:
        print("Đã hoàn thành tất cả các trang!")
        return all_links
    
    driver = setup_driver()
    
    try:
        # Lặp qua các trang cần cào
        for page_num in tqdm(range(start_page, end_page + 1), desc="Đang xử lý các trang"):
            # Cào dữ liệu từ trang hiện tại
            page_links = crawl_page(driver, page_num)
            
            # Thêm vào danh sách tổng
            all_links.extend(page_links)
            
            # Lưu tiến độ sau mỗi 5 trang hoặc trang cuối
            if page_num % 5 == 0 or page_num == end_page:
                save_progress(all_links, page_num)
            
            # Nghỉ ngắn giữa các trang để tránh bị chặn
            time.sleep(2)
    
    except KeyboardInterrupt:
        print("\nĐã dừng quá trình cào dữ liệu bởi người dùng!")
        # Lưu tiến độ hiện tại trước khi dừng
        current_page = start_page + len(all_links) - 1
        save_progress(all_links, current_page)
    
    except Exception as e:
        print(f"Lỗi không xác định: {e}")
    
    finally:
        # Đóng trình duyệt
        driver.quit()
        
        # Loại bỏ các liên kết trùng lặp
        all_links = list(dict.fromkeys(all_links))
        
        # Lưu kết quả cuối cùng
        with open('linkProduct.json', 'w', encoding='utf-8') as f:
            json.dump(all_links, f, ensure_ascii=False, indent=4)
        
        print(f"\nHoàn thành! Đã lưu tổng cộng {len(all_links)} liên kết vào file linkProduct.json")
        
        return all_links

if __name__ == "__main__":
    # Mặc định sẽ cào từ trang 1 đến 2895
    # Có thể thay đổi thành một khoảng nhỏ hơn để kiểm tra, ví dụ: crawl_batdongsan(1, 10)
    crawl_batdongsan(1, 2895)