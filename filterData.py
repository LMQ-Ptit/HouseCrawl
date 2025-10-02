import pandas as pd
import json
import os

def remove_duplicates_from_json(input_file='linkProduct.json', output_file=None):
    """
    Đọc dữ liệu từ file JSON, lọc các giá trị trùng lặp và lưu lại kết quả.
    
    Parameters:
        input_file (str): Đường dẫn đến file JSON đầu vào
        output_file (str): Đường dẫn đến file JSON đầu ra. Nếu None, sẽ ghi đè lên file đầu vào
    
    Returns:
        int: Số lượng phần tử sau khi lọc
    """
    # Kiểm tra file tồn tại
    if not os.path.exists(input_file):
        print(f"Lỗi: File {input_file} không tồn tại.")
        return 0
    
    try:
        # Đọc dữ liệu từ file JSON
        print(f"Đang đọc dữ liệu từ {input_file}...")
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Hiển thị thông tin trước khi lọc
        initial_count = len(data)
        print(f"Số lượng liên kết ban đầu: {initial_count}")
        
        # Chuyển đổi thành DataFrame pandas
        df = pd.DataFrame(data, columns=['link'])
        
        # Lọc các giá trị trùng lặp
        df_unique = df.drop_duplicates()
        
        # Chuyển lại thành list
        unique_data = df_unique['link'].tolist()
        
        # Hiển thị kết quả
        final_count = len(unique_data)
        print(f"Số lượng liên kết sau khi lọc: {final_count}")
        print(f"Đã loại bỏ {initial_count - final_count} liên kết trùng lặp")
        
        # Xác định file đầu ra
        if output_file is None:
            output_file = input_file
        
        # Lưu dữ liệu đã lọc
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(unique_data, f, ensure_ascii=False, indent=4)
        
        print(f"Đã lưu dữ liệu đã lọc vào {output_file}")
        
        return final_count
    
    except Exception as e:
        print(f"Lỗi khi xử lý file: {e}")
        return 0

def convert_json_to_csv(input_file='linkProduct.json', output_file='linkProduct.csv'):
    """
    Chuyển đổi dữ liệu từ file JSON sang file CSV.
    
    Parameters:
        input_file (str): Đường dẫn đến file JSON đầu vào
        output_file (str): Đường dẫn đến file CSV đầu ra
    
    Returns:
        bool: True nếu chuyển đổi thành công, False nếu có lỗi
    """
    try:
        # Kiểm tra file tồn tại
        if not os.path.exists(input_file):
            print(f"Lỗi: File {input_file} không tồn tại.")
            return False
            
        # Đọc dữ liệu từ file JSON
        print(f"Đang đọc dữ liệu từ {input_file}...")
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Kiểm tra xem dữ liệu là list các chuỗi hay dictionary
        if isinstance(data, list):
            if all(isinstance(item, str) for item in data):
                # Danh sách các URL
                df = pd.DataFrame(data, columns=['url'])
            else:
                # Danh sách các dictionary
                df = pd.DataFrame(data)
        elif isinstance(data, dict):
            # Dữ liệu là dictionary
            df = pd.DataFrame([data])
        else:
            print(f"Không hỗ trợ định dạng dữ liệu: {type(data)}")
            return False
            
        # Lưu thành file CSV
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Đã chuyển đổi thành công từ JSON sang CSV.")
        print(f"Đã lưu {len(df)} dòng dữ liệu vào file {output_file}")
        
        return True
        
    except Exception as e:
        print(f"Lỗi khi chuyển đổi file: {e}")
        return False

if __name__ == "__main__":
    convert_json_to_csv()