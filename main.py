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
from tqdm import tqdm  # Thêm thư viện tqdm

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

def crawl_property_info(url):
    """Cào thông tin bất động sản từ URL cụ thể"""
    driver = setup_driver()
    property_data = {}  
    
    try:
        print(f"Đang truy cập URL: {url}")
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
        return df
            
    except Exception as e:
        print(f"Lỗi khi cào dữ liệu: {e}")
        # Vẫn trả về DataFrame với URL để ghi lại thông tin thất bại
        return pd.DataFrame([{"URL": url, "Error": str(e)}])
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
                print(f"Phát hiện cấu trúc cột khác nhau. Đang kết hợp dữ liệu...")
                # Kết hợp DataFrame mới với dữ liệu hiện có
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                
                # Ghi lại toàn bộ file với thanh tiến trình
                print(f"Đang ghi {len(combined_df)} bản ghi vào file...")
                combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"✅ Đã lưu lô thứ {batch_count}: {len(df)} bản ghi mới, tổng {len(combined_df)} bản ghi")
                return
            
            # Nếu cấu trúc cột giống nhau, chỉ cần append
            df.to_csv(output_file, mode='a', header=False, index=False, encoding='utf-8-sig')
            print(f"✅ Đã lưu lô thứ {batch_count}: {len(df)} bản ghi")
            
        except Exception as e:
            print(f"❌ Lỗi khi xử lý file: {e}")
            # Nếu có lỗi, ghi đè file
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"Đã ghi đè file với lô dữ liệu thứ {batch_count}")
    else:
        # Nếu file chưa tồn tại, tạo mới
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"✅ Đã tạo file mới và lưu {len(df)} bản ghi vào {output_file}")

def process_urls_in_batches(urls, batch_size=1000, output_file='property_data.csv'):
    """Xử lý danh sách URL theo lô"""
    results = []
    batch_count = 1
    total_urls = len(urls)
    
    # Sử dụng tqdm để hiển thị thanh tiến trình
    for i, url in enumerate(tqdm(urls, desc="Tiến trình cào dữ liệu", unit="URL")):
        # Hiển thị URL đang xử lý trên cùng một dòng
        tqdm.write(f"Đang xử lý [{i+1}/{total_urls}]: {url}")
        
        try:
            # Cào dữ liệu
            df_result = crawl_property_info(url)
            
            if not df_result.empty:
                # Thêm vào danh sách kết quả
                results.append(df_result)
                tqdm.write(f"  ✓ Đã cào thành công. Thuộc tính: {len(df_result.columns)}")
            
            # Khi đủ số lượng trong batch hoặc đây là URL cuối cùng
            if len(results) >= batch_size or i == total_urls - 1:
                if results:  # Kiểm tra nếu có kết quả
                    # Hiển thị thanh tiến trình khi kết hợp DataFrame
                    tqdm.write(f"\n🔄 Đang xử lý lô thứ {batch_count}...")
                    
                    # Kết hợp các DataFrame lại với nhau
                    combined_df = pd.concat(results, ignore_index=True)
                    
                    # Ghi vào file CSV
                    append_to_csv(combined_df, output_file, batch_count)
                    
                    # Làm trống danh sách kết quả
                    results = []
                    batch_count += 1
            
            # # Thêm thời gian nghỉ giữa các lần gọi để tránh bị chặn
            # wait_time = random.uniform(1, 1.5)
            # time.sleep(wait_time)
            
        except Exception as e:
            tqdm.write(f"  ❌ Lỗi: {e}")
            # Ghi lại URL lỗi
            error_df = pd.DataFrame([{"URL": url, "Error": str(e)}])
            results.append(error_df)

if __name__ == "__main__":
    # File đầu vào và đầu ra
    input_csv = 'linkProduct.csv'
    output_csv = 'property_data.csv'
    batch_size = 10
    
    # Hiển thị tiêu đề với màu sắc
    print("\n" + "="*70)
    print("🏠 BẮT ĐẦU CÀO DỮ LIỆU BẤT ĐỘNG SẢN")
    print("="*70 + "\n")
    
    # Đọc các URL từ file CSV
    print("📂 Đang đọc danh sách URL...")
    urls = read_urls_from_csv(input_csv)
    
    if urls:
        print(f"🔍 Đã tìm thấy {len(urls)} URL để xử lý, batch size: {batch_size}\n")
        
        # Hiển thị thời gian bắt đầu
        start_time = time.time()
        
        # Xử lý URL
        process_urls_in_batches(urls, batch_size, output_csv)
        
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