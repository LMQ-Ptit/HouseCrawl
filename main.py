from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import pandas as pd
import os
import random
from tqdm import tqdm
import multiprocessing
from multiprocessing import Pool, Manager, Lock
import functools

def setup_driver():
    """Khởi tạo trình duyệt Chrome"""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--log-level=3")  # Tắt log của Chrome
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
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
    property_data = {}
    
    try:
        driver.get(url)
        wait_time = random.uniform(1, 1.5)
        # Đợi trang tải xong
        time.sleep(wait_time)
        
        # Tìm tất cả các thẻ div có class="re__pr-specs-content-item"
        spec_items = driver.find_elements(By.CSS_SELECTOR, "div.re__pr-specs-content-item")
        
        for item in spec_items:
            text = item.text.strip()
            if "\n" in text:
                # Tách dòng đầu tiên làm tên thuộc tính, phần còn lại là giá trị
                parts = text.split("\n", 1)
                property_name = parts[0].strip()
                property_value = parts[1].strip()
                property_data[property_name] = property_value
        
        # Tìm thẻ div có class="re__pr-short-info-item js__pr-config-item"
        short_info_items = driver.find_elements(By.CSS_SELECTOR, "div.re__pr-short-info-item.js__pr-config-item")
        
        for item in short_info_items:
            text = item.text.strip()
            if "\n" in text:
                parts = text.split("\n", 1)
                property_name = parts[0].strip()
                property_value = parts[1].strip()
                property_data[property_name] = property_value
        
        return property_data, None
            
    except Exception as e:
        error_message = str(e)
        return {"URL": url, "Error": error_message}, error_message
    finally:
        driver.quit()

def read_urls_from_csv(csv_file='linkProduct.csv'):
    """Đọc danh sách URL từ file CSV"""
    try:
        # Kiểm tra nếu file tồn tại
        if not os.path.exists(csv_file):
            print(f"File {csv_file} không tồn tại.")
            
            # Kiểm tra xem có file JSON không để chuyển đổi
            json_file = csv_file.replace('.csv', '.json')
            if os.path.exists(json_file):
                print(f"Tìm thấy file JSON {json_file}, đang chuyển đổi thành CSV...")
                with open(json_file, 'r', encoding='utf-8') as f:
                    urls = json.load(f)
                
                df = pd.DataFrame({'url': urls})
                df.to_csv(csv_file, index=False)
                print(f"Đã chuyển đổi thành công {len(urls)} URL sang file {csv_file}")
                return urls
            return []
            
        # Đọc file CSV
        df = pd.read_csv(csv_file)
        
        # Tìm cột chứa URL
        url_column = None
        for col in ['url', 'URL', 'link', 'Link']:
            if col in df.columns:
                url_column = col
                break
        
        if not url_column and len(df.columns) > 0:
            url_column = df.columns[0]
        
        if url_column:
            urls = df[url_column].tolist()
            print(f"Đã đọc {len(urls)} URL từ file {csv_file}")
            return urls
        else:
            print(f"Không tìm thấy cột URL trong file {csv_file}")
            return []
            
    except Exception as e:
        print(f"Lỗi khi đọc file CSV: {e}")
        return []

def append_to_csv_safe(df, output_file, file_lock, batch_info):
    """Ghi DataFrame vào file CSV với khóa đồng bộ hóa giữa các tiến trình"""
    # Sử dụng khóa để đảm bảo chỉ một tiến trình ghi file tại một thời điểm
    with file_lock:
        process_id, batch_id = batch_info
        batch_count = f"{process_id}-{batch_id}"
        
        # Kiểm tra xem file đã tồn tại chưa
        file_exists = os.path.exists(output_file) and os.path.getsize(output_file) > 0
        
        if file_exists:
            try:
                # Đọc header của file hiện có để kiểm tra cấu trúc
                existing_df = pd.read_csv(output_file)
                
                # So sánh cấu trúc cột
                existing_cols = set(existing_df.columns)
                new_cols = set(df.columns)
                
                if existing_cols != new_cols:
                    print(f"[Tiến trình {process_id}] Cấu trúc cột khác nhau. Đang kết hợp dữ liệu...")
                    # Kết hợp DataFrame mới với dữ liệu hiện có
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    
                    # Ghi lại toàn bộ file
                    combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
                    print(f"[Tiến trình {process_id}] 💾 Đã lưu lô {batch_count}: {len(df)} bản ghi mới, tổng {len(combined_df)} bản ghi")
                    return
                
                # Nếu cấu trúc cột giống nhau, chỉ cần append
                df.to_csv(output_file, mode='a', header=False, index=False, encoding='utf-8-sig')
                print(f"[Tiến trình {process_id}] 💾 Đã lưu lô {batch_count}: {len(df)} bản ghi")
                
            except Exception as e:
                print(f"[Tiến trình {process_id}] ❌ Lỗi khi xử lý file: {e}")
                # Nếu có lỗi, ghi đè file
                df.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"[Tiến trình {process_id}] Đã ghi đè file với lô {batch_count}")
        else:
            # Nếu file chưa tồn tại, tạo mới
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"[Tiến trình {process_id}] 💾 Đã tạo file mới và lưu {len(df)} bản ghi vào {output_file}")

def process_url_batch(batch_data):
    """Hàm xử lý cho mỗi tiến trình"""
    process_id, url_batch, output_file, file_lock = batch_data
    
    print(f"[Tiến trình {process_id}] Bắt đầu xử lý {len(url_batch)} URLs")
    batch_size = 10  # Ghi file sau mỗi 10 URL
    total_urls = len(url_batch)
    results = []
    
    # Xử lý URLs theo batch nhỏ
    for i, url in enumerate(url_batch):
        position = i + 1
        print(f"[Tiến trình {process_id}] [{position}/{total_urls}] Đang xử lý: {url}")
        
        property_data, error = crawl_property_info(url)
        
        if error:
            print(f"[Tiến trình {process_id}] ❌ Lỗi: {error}")
        else:
            results.append(property_data)
            print(f"[Tiến trình {process_id}] ✅ Xử lý thành công")
        
        # Ghi kết quả sau mỗi batch_size hoặc khi đến URL cuối cùng
        if len(results) >= batch_size or i == total_urls - 1:
            if results:
                batch_id = i // batch_size + 1
                batch_info = (process_id, batch_id)
                df = pd.DataFrame(results)
                append_to_csv_safe(df, output_file, file_lock, batch_info)
                results = []  # Reset danh sách kết quả sau khi ghi
    
    print(f"[Tiến trình {process_id}] Hoàn thành xử lý tất cả URLs")
    return process_id

def process_with_multiprocessing(urls, num_processes=4, output_file='property_data.csv'):
    """Xử lý các URL với đa tiến trình"""
    # Khởi tạo Manager để chia sẻ khóa giữa các tiến trình
    with Manager() as manager:
        file_lock = manager.Lock()
        
        # Chia URLs thành các nhóm cho từng tiến trình
        chunk_size = len(urls) // num_processes
        url_batches = []
        
        for i in range(num_processes):
            start_idx = i * chunk_size
            # Đối với tiến trình cuối cùng, lấy tất cả các URL còn lại
            end_idx = (i + 1) * chunk_size if i < num_processes - 1 else len(urls)
            url_batches.append((i + 1, urls[start_idx:end_idx], output_file, file_lock))
        
        print(f"Khởi tạo {num_processes} tiến trình, mỗi tiến trình xử lý {chunk_size} URLs")
        
        # Khởi tạo và chạy các tiến trình
        with Pool(processes=num_processes) as pool:
            # Bắt đầu xử lý và chờ kết quả
            for process_id in pool.imap_unordered(process_url_batch, url_batches):
                print(f"Tiến trình {process_id} đã hoàn thành")

if __name__ == "__main__":
    # File đầu vào và đầu ra    
    input_csv = 'linkProduct.csv'
    output_csv = 'property_data.csv'
    num_processes = 8  # Số tiến trình xử lý đồng thời
    
    # Hiển thị tiêu đề
    print("\n" + "="*70)
    print("🏠 BẮT ĐẦU CÀO DỮ LIỆU BẤT ĐỘNG SẢN ĐA TIẾN TRÌNH")
    print("="*70 + "\n")
    
    # Đọc các URL từ file CSV
    print("📂 Đang đọc danh sách URL...")
    urls = read_urls_from_csv(input_csv)
    
    if urls:
        print(f"🔍 Đã tìm thấy {len(urls)} URL để xử lý với {num_processes} tiến trình\n")
        
        # Ghi lại thời gian bắt đầu
        start_time = time.time()
        
        # Xử lý URL với đa tiến trình
        process_with_multiprocessing(urls[2924:], num_processes, output_csv)
        
        # Tính thời gian thực hiện
        elapsed_time = time.time() - start_time
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Hiển thị thông báo hoàn thành
        print("\n" + "="*70)
        print(f"✅ HOÀN THÀNH! Thời gian: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
        print(f"📊 Dữ liệu đã được lưu vào: {output_csv}")
        print("="*70)
    else:
        print("❌ Không có URL để xử lý.")