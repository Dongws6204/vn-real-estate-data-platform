import requests
import concurrent.futures
import time
from typing import List


RAW_PROXIES_FILE = 'proxies_raw.txt'    # File chứa list proxy gốc mày lượm được
GOOD_PROXIES_FILE = 'proxies_good.txt'   # File output chứa các proxy còn sống
CHECK_URL = 'https://batdongsan.com.vn/' # Check thẳng vào trang đích, thực tế nhất!
TIMEOUT = 5                              # Timeout 5 giây là đủ cho một proxy tốt
MAX_WORKERS = 20                         # Số luồng chạy song song (số "công nhân")

def check_proxy(proxy: str) -> bool:
    """Kiểm tra một proxy có hoạt động với URL đích hay không."""
    proxies = {
        'http': f'http://{proxy}',
        'https-': f'http://{proxy}' # curl_cffi dùng http-proxy cho cả https
    }
    try:
        # Dùng thư viện requests đơn giản để check cho nhanh, không cần curl_cffi ở đây
        response = requests.get(CHECK_URL, proxies=proxies, timeout=TIMEOUT, verify=False)
        # Check xem có bị chặn hay không (mã 200 là OK, 403 là bị chặn)
        if response.status_code == 200:
            print(f"✅ SUCCESS: Proxy {proxy} is working!")
            return True
        else:
            print(f"❌ FAILED : Proxy {proxy} returned status {response.status_code}.")
            return False
    except requests.exceptions.RequestException as e:
        return False

def main():
    print("--- Starting Proxy Checker ---")
    try:
        with open(RAW_PROXIES_FILE, 'r') as f:
            raw_proxies = [line.strip() for line in f if line.strip()]
        if not raw_proxies:
            print(f"Error: {RAW_PROXIES_FILE} is empty.")
            return
        print(f"Found {len(raw_proxies)} proxies in {RAW_PROXIES_FILE}. Start checking...")
    except FileNotFoundError:
        print(f"Error: Please create a file named '{RAW_PROXIES_FILE}' and paste your proxies in it.")
        return

    start_time = time.time()
    good_proxies = []

    # Sử dụng ThreadPoolExecutor để chạy kiểm tra song song
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Gửi tất cả các task check proxy vào pool
        future_to_proxy = {executor.submit(check_proxy, proxy): proxy for proxy in raw_proxies}
        
        # Lấy kết quả khi chúng hoàn thành
        for future in concurrent.futures.as_completed(future_to_proxy):
            proxy = future_to_proxy[future]
            try:
                if future.result():
                    good_proxies.append(proxy)
            except Exception as exc:
                print(f"Proxy {proxy} generated an exception: {exc}")

    end_time = time.time()
    
    print("\n--- Check Completed ---")
    print(f"Total time: {end_time - start_time:.2f} seconds")
    print(f"Found {len(good_proxies)} working proxies.")

    if good_proxies:
        with open(GOOD_PROXIES_FILE, 'w') as f:
            for proxy in good_proxies:
                f.write(f"{proxy}\n")
        print(f"Working proxies have been saved to '{GOOD_PROXIES_FILE}'")
    else:
        print("No working proxies found. You might need a new list of proxies.")

if __name__ == "__main__":
    main()