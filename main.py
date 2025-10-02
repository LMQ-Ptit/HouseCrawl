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
from concurrent.futures import ThreadPoolExecutor
import threading

# Khóa để đồng bộ hóa ghi vào file CSV
file_lock = threading.Lock()

def setup_driver():
    """Khởi tạo trình duyệt Chrome"""
    options = Options()
    options.add_argument("--headless")
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

def crawl_property_info(url, position=0, total=0):
    """Cào thông tin bất động sản từ URL cụ thể"""
    driver = setup_driver()
    property_data = {}  
    
    try:
        tqdm.write(f"[{position}/{total}] Đang truy cập: {url}")
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
        
        # Chuyển dictionary thành DataFrame
        df = pd.DataFrame([property_data])
        return df, None  # Trả về DataFrame và không có lỗi
            
    except Exception as e:
        error_msg = str(e)
        tqdm.write(f"  ❌ Lỗi: {error_msg}")
        # Vẫn trả về DataFrame với URL để ghi lại thông tin thất bại
        return pd.DataFrame([{"URL": url, "Error": error_msg}]), error_msg
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
        
        # Tìm cột chứa URL (có thể là 'url', 'link', hoặc cột đầu tiên)
        url_column = None
        for col in ['url', 'URL', 'link', 'Link']:
            if col in df.columns:
                url_column = col
                break
        
        if not url_column and len(df.columns) > 0:
            # Nếu không tìm thấy cột tên cụ thể, sử dụng cột đầu tiên
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

def append_to_csv(df, output_file='property_data.csv', batch_count=1):
    """
    Ghi DataFrame vào file CSV đảm bảo tương thích với dữ liệu hiện có
    
    Parameters:
        df (DataFrame): DataFrame cần ghi
        output_file (str): Đường dẫn đến file CSV đầu ra
        batch_count (int): Số batch để hiển thị trong thông báo
    """
    # Sử dụng lock để tránh đụng độ khi nhiều luồng cùng ghi file
    with file_lock:
        # Kiểm tra xem file đã tồn tại chưa
        file_exists = os.path.exists(output_file)
        
        if file_exists:
            try:
                # Đọc header của file hiện có để kiểm tra cấu trúc
                existing_df = pd.read_csv(output_file)
                
                # So sánh cấu trúc cột
                existing_cols = set(existing_df.columns)
                new_cols = set(df.columns)
                
                if existing_cols != new_cols:
                    tqdm.write(f"Phát hiện cấu trúc cột khác nhau. Đang kết hợp dữ liệu...")
                    # Kết hợp DataFrame mới với dữ liệu hiện có
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    
                    # Ghi lại toàn bộ file với thanh tiến trình
                    tqdm.write(f"Đang ghi {len(combined_df)} bản ghi vào file...")
                    combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
                    tqdm.write(f"✅ Đã lưu lô thứ {batch_count}: {len(df)} bản ghi mới, tổng {len(combined_df)} bản ghi")
                    return
                
                # Nếu cấu trúc cột giống nhau, chỉ cần append
                df.to_csv(output_file, mode='a', header=False, index=False, encoding='utf-8-sig')
                tqdm.write(f"✅ Đã lưu lô thứ {batch_count}: {len(df)} bản ghi")
                
            except Exception as e:
                tqdm.write(f"❌ Lỗi khi xử lý file: {e}")
                # Nếu có lỗi, ghi đè file
                df.to_csv(output_file, index=False, encoding='utf-8-sig')
                tqdm.write(f"Đã ghi đè file với lô dữ liệu thứ {batch_count}")
        else:
            # Nếu file chưa tồn tại, tạo mới
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            tqdm.write(f"✅ Đã tạo file mới và lưu {len(df)} bản ghi vào {output_file}")

# Hàm mới để xử lý một lô URLs
def process_batch(urls_batch, output_file, batch_idx, total_batches):
    results = []
    batch_size = len(urls_batch)
    
    # Cào dữ liệu cho tất cả URL trong lô
    for idx, url in enumerate(urls_batch):
        df_result, error = crawl_property_info(url, idx+1, batch_size)
        if not df_result.empty:
            results.append(df_result)
    
    # Nếu có kết quả, kết hợp và lưu
    if results:
        combined_df = pd.concat(results, ignore_index=True)
        append_to_csv(combined_df, output_file, batch_idx)

# Hàm mới để xử lý đa luồng
def process_with_multithreading(urls, num_threads=4, batch_size=10, output_file='property_data.csv'):
    """Xử lý danh sách URL với đa luồng"""
    total_urls = len(urls)
    
    # Chia danh sách URL thành các lô
    batches = [urls[i:i + batch_size] for i in range(0, total_urls, batch_size)]
    num_batches = len(batches)
    
    print(f"\n🧵 Sử dụng {num_threads} luồng để xử lý {total_urls} URL trong {num_batches} lô")
    
    # Sử dụng ThreadPoolExecutor để chạy đa luồng
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit các tác vụ và lấy các Future objects
        futures = []
        for i, batch in enumerate(batches):
            future = executor.submit(process_batch, batch, output_file, i+1, num_batches)
            futures.append(future)
        
        # Theo dõi tiến trình với tqdm
        with tqdm(total=num_batches, desc="Tiến trình xử lý các lô") as pbar:
            # Đếm số lô đã hoàn thành
            completed = 0
            # Kiểm tra tiến trình của các future
            while completed < num_batches:
                new_completed = sum(1 for f in futures if f.done())
                if new_completed > completed:
                    pbar.update(new_completed - completed)
                    completed = new_completed
                time.sleep(0.1)

if __name__ == "__main__":
    # File đầu vào và đầu ra
    input_csv = 'linkProduct.csv'
    output_csv = 'property_data.csv'
    batch_size = 10
    num_threads = 4  # Số luồng xử lý đồng thời
    
    # Hiển thị tiêu đề
    print("\n" + "="*70)
    print("🏠 BẮT ĐẦU CÀO DỮ LIỆU BẤT ĐỘNG SẢN ĐA LUỒNG")
    print("="*70 + "\n")
    
    # Đọc các URL từ file CSV
    print("📂 Đang đọc danh sách URL...")
    urls = read_urls_from_csv(input_csv)
    
    if urls:
        print(f"🔍 Đã tìm thấy {len(urls)} URL để xử lý, batch size: {batch_size}, threads: {num_threads}\n")
        
        # Hiển thị thời gian bắt đầu
        start_time = time.time()
        
        # Xử lý URL với đa luồng
        process_with_multithreading(urls, num_threads, batch_size, output_csv)
        
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