import re
import sys
from bs4 import BeautifulSoup
from curl_cffi import requests

# Cấu hình mặc định để test
DEFAULT_URL = "https://nhadat.cafeland.vn/von-chi-800tr-20-so-huu-luon-can-73m-tmdv-trung-tam-cau-giay-goi-0941617318-2288873.html"

def crawl_cafeland_detail(url):
    """
    Hàm thực hiện cào chi tiết một tin bất động sản từ Cafeland.
    """
    print(f"\n{'='*50}")
    print(f"🚀 Đang truy cập: {url}")
    print(f"{'='*50}")

    # Sử dụng curl_cffi để giả lập Chrome 120 (vượt Cloudflare)
    try:
        response = requests.get(url, impersonate="chrome120", timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Lỗi khi tải trang: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    # ===== 1. TIÊU ĐỀ =====
    title_tag = soup.select_one('h1.head-title')
    print("\n[1] TIÊU ĐỀ")
    print(f"Title: {title_tag.get_text(strip=True) if title_tag else 'N/A'}")

    # ===== 2. ĐỊA CHỈ =====
    print("\n[2] ĐỊA CHỈ")
    location_div = soup.select_one('div.infor')
    if location_div:
        loc_clone = BeautifulSoup(str(location_div), 'html.parser').div
        for junk in loc_clone.find_all("div"):
            junk.decompose()
        fragments = list(loc_clone.stripped_strings)
        address_elements = [f for f in fragments if f not in ["Location:", "▸", '"', '", "']]
        full_address = ", ".join(address_elements).replace(", ,", ",").strip(", ")
        print(f"Địa chỉ sạch: {full_address}")
    else:
        print("N/A")
    # ===== 3. ĐẶC ĐIỂM =====
    print("\n[3] ĐẶC ĐIỂM")
    
    # Selector này sẽ hốt hết tất cả các thẻ chứa thông tin đặc điểm
    # Em dùng dấu phẩy để chọn nhiều loại class cùng lúc cho chắc ăn
    features = soup.select('div.reals-house-item, div.col-item, .reals-info-group .col-item')
    
    if features:
        # Dùng set() để lưu các đặc điểm đã in, tránh bị trùng lặp nếu selector quét dính 2 lần
        seen_features = set()
        
        for feat in features:
            # Tìm nhãn (ví dụ: Hướng nhà, Pháp lý, Số tầng...)
            label_tag = feat.select_one('.title-item, .infor-note')
            # Tìm giá trị (ví dụ: Đông Nam, Sổ hồng, 3 tầng...)
            value_tag = feat.select_one('.value-item, .infor-data')
            
            if label_tag:
                label = label_tag.get_text(strip=True)
                
                # Nếu tìm thấy nhãn này rồi thì bỏ qua không in lại
                if label in seen_features:
                    continue
                
                # Lấy nội dung giá trị, nếu không có thì để là "N/A"
                value = value_tag.get_text(strip=True) if value_tag else "N/A"
                if not value: value = "N/A" # Trường hợp có thẻ <span></span> nhưng rỗng
                
                print(f" - {label}: {value}")
                seen_features.add(label)
    else:
        print("Không tìm thấy cụm đặc điểm nào.")

    # ===== 4. MÔ TẢ =====
    print("\n[4] MÔ TẢ")
    desc_div = soup.select_one('div.reals-description div.blk-content')
    if desc_div:
        desc_text = desc_div.get_text(separator="\n", strip=True)
        print(f"Độ dài: {len(desc_text)} ký tự")
        print(f"Nội dung (trích đoạn): {desc_text[:200]}...")
    else:
        print("Không thấy phần mô tả.")

    # ===== 5. THÔNG TIN LIÊN HỆ =====
    print("\n[5] LIÊN HỆ")
    contact = soup.select_one('div.profile-info, div.block-contact-infor')
    if contact:
        name = contact.select_one('.profile-name h2')
        print(f"Người đăng: {name.get_text(strip=True) if name else 'N/A'}")

        phone_el = contact.select_one('.profile-phone span')
        if phone_el and phone_el.get('onclick'):
            phone_raw = re.search(r"'(0\d+\*+)'", phone_el['onclick'])
            print(f"Số ĐT (Mặt nạ): {phone_raw.group(1) if phone_raw else 'N/A'}")

        email_el = contact.select_one('.profile-email span')
        if email_el:
            u = email_el.get('data-hidden-name', '')
            d = email_el.get('data-hidden-domain', '')
            print(f"Email: {u}@{d}" if u else "Email: N/A")
    else:
        print("Không thấy block liên hệ.")

    # ===== 6. MEDIA =====
    print("\n[6] MEDIA")
    carousel = soup.select_one('div.carousel-inner')
    if carousel:
        imgs = []
        pl = carousel.find('link', rel='preload')
        if pl: imgs.append(pl['href'])
        for a in carousel.select('a.lg-item'):
            if a.get('href') and a['href'] not in imgs:
                imgs.append(a['href'])
        for i, img in enumerate(imgs[:3]):
            print(f"Ảnh {i+1}: {img}")
        video = carousel.select_one('a.videoks')
        if video: print(f"Video URL: {video.get('data-url')}")
    else:
        print("N/A")

# ===== 7. TỌA ĐỘ =====
    print("\n[7] TỌA ĐỘ")
    # Cách 1: Tìm qua iframe như cũ nhưng thêm selector class mới
    map_iframe = soup.select_one('iframe[src*="maps.google.com"], .frame-map iframe, .reals-map iframe')
    
    found_coords = False
    if map_iframe and (src := map_iframe.get('src')):
        coords = re.search(r'q=([\d\.]+),([\d\.]+)', src)
        if coords:
            print(f"Tọa độ (từ iframe): Lat={coords.group(1)}, Lon={coords.group(2)}")
            found_coords = True

    # Cách 2: Nếu cách 1 tạch, quét "láo" toàn bộ text của trang web
    if not found_coords:
        # Tìm các chuỗi có định dạng số.số , số.số (tọa độ GPS)
        # Quét cụm tọa độ đặc trưng của VN (Vĩ độ 10-21, Kinh độ 102-109)
        raw_coords = re.findall(r'(21\.\d+|10\.\d+),\s*(105\.\d+|106\.\d+|107\.\d+)', response.text)
        if raw_coords:
            lat, lon = raw_coords[0]
            print(f"Tọa độ (quét thô): Lat={lat}, Lon={lon}")
            found_coords = True

    if not found_coords:
        print("Không tìm thấy tọa độ qua iframe hay text thô.")

# ===== 8. METADATA =====
    print("\n[8] METADATA")
    meta_div = soup.select_one('div.col-right div.infor')
    
    if meta_div:
        # Lấy text thô
        meta_text = meta_div.get_text(" ", strip=True)
        print(f"Text thô: {meta_text}")

        # --- FIX REGEX TẠI ĐÂY ---
        # Tìm mã tin: Quét sau chữ "Mã tài sản:" hoặc "Asset Code:" lấy dãy số
        asset_id = re.search(r"(?:Mã tài sản|Asset Code):\s*(\d+)", meta_text)
        
        # Tìm ngày đăng: Quét sau chữ "Ngày đăng:" hoặc "Date posted:" lấy định dạng dd-mm-yyyy
        post_date = re.search(r"(?:Ngày đăng|Date posted):\s*(\d{2}-\d{2}-\d{4})", meta_text)
        
        id_val = asset_id.group(1) if asset_id else "N/A"
        date_val = post_date.group(1) if post_date else "N/A"
        
        print(f"Mã tin: {id_val}")
        print(f"Ngày đăng: {date_val}")
    else:
        print("Không tìm thấy thẻ Metadata")

    
    print(f"\n{'='*50}")
    print("✅ Hoàn thành crawl.")

if __name__ == "__main__":
    # Cho phép truyền URL qua dòng lệnh, nếu không có sẽ dùng URL mặc định
    target_url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL
    
    try:
        crawl_cafeland_detail(target_url)
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng chương trình.")
        sys.exit(0)