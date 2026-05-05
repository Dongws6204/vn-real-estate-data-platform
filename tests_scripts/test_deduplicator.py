import copy
import pytest
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.deduplicator import Deduplicator
from datetime import datetime

# Dữ liệu mẫu giả lập (sắp xếp theo các trường hợp)
@pytest.fixture
def sample_listings():
    # Basic: giống như chỉ lấy từ listing scraper (thiếu desc)
    basic_listing = {
        'source': 'raovat321',
        'source_id': '123',
        'title': 'Nhà quận 1 đẹp',
        'url': 'https://raovat321.vn/chi-tiet/123',
        'image_url': 'https://raovat321.vn/images/123.jpg',
        'price': 1000000000.0,
        'posted_time': '1 ngày trước',
        'categories': ['Bán nhà', 'TP.HCM'],
        'location': {
            'district': 'Quận 1',
            'city': 'TP.HCM',
        },
        'is_vip': False,
        'crawled': False,
        'crawled_at': datetime(2023, 10, 1),
        'created_at': datetime(2023, 10, 1),
        'updated_at': datetime(2023, 10, 1),
    }

    # Full listing: giả định đã có thêm desc từ detail scraper
    full_listing = {
        **basic_listing,
        'source_id': '456',
        'title': 'Căn hộ quận 1 xinh xắn',
        'description': 'Nhà đẹp, gần trung tâm, có sân vườn rộng.',
        'price': 1050000000.0,
        'location': {
            'district': 'Q1',
            'city': 'Sài Gòn',
        },
        'crawled_at': datetime(2023, 10, 2),
        'created_at': datetime(2023, 10, 2),
        'updated_at': datetime(2023, 10, 2),
    }

    # Dupe giống với full_listing (thay đổi nhẹ)
    similar_dupe = {
        **full_listing,
        'source_id': '123-dupe',
        'title': 'Nhà quận 1 đẹp giá rẻ',
        'description': 'Nhà xinh gần trung tâm, sân vườn lớn.',
        'price': 950000000.0,
        'location': {
            'district': 'Quận 1',
            'city': 'TP.HCM',
        },
        'crawled_at': datetime(2023, 10, 3),
        'updated_at': datetime(2023, 10, 3),
    }

    # Một listing không trùng
    no_dupe = {
        'source': 'raovat321',
        'source_id': '789',
        'title': 'Đất Bình Dương',
        'description': 'Đất rộng, xa thành phố.',
        'url': 'https://raovat321.vn/chi-tiet/789',
        'image_url': 'https://raovat321.vn/images/789.jpg',
        'price': 500000000.0,
        'posted_time': '3 ngày trước',
        'categories': ['Bán đất', 'Bình Dương'],
        'location': {
            'district': 'Thủ Dầu Một',
            'city': 'Bình Dương',
        },
        'is_vip': False,
        'crawled': False,
        'crawled_at': datetime(2023, 10, 1),
        'created_at': datetime(2023, 10, 1),
        'updated_at': datetime(2023, 10, 1),
    }

    return {
        'basic': basic_listing,
        'full': full_listing,
        'similar_dupe': similar_dupe,
        'no_dupe': no_dupe
    }
prices = [
    {
        "amount": 8.9,
        "currency": "VND",
        "unit": "billion",
        "original_text": "8,9 tỷ"
    },
    {
        "amount": 9.1,
        "currency": "VND",
        "unit": "billion",
        "original_text": "9,1 tỷ"
    }
]

@pytest.fixture
def deduplicator_real():
    return Deduplicator()

# def test_get_price_label(deduplicator_real):
#     result = True
#     price_label = deduplicator_real._get_price_label(prices[0])
#     print(price_label)
#     if not isinstance(price_label, str):
#         result = False
#     # for price in prices:
#     #     price_label = deduplicator_real._get_price_label(price)
#     #     print (price_label)
#     #     if not isinstance(price_label, str): 
#     #         result = False

#     assert result is True  # sim thấp

# def test_is_similar_identical_strings(deduplicator_real):
#     result = deduplicator_real.is_similar("first listing", "first listing")
#     assert result is True  # Embedding giống nhau -> sim ≈ 1.0 >= 0.85

# def test_is_similar_different_strings(deduplicator_real):
#     result = deduplicator_real.is_similar("first listing", "completely different listing")
#     assert result is False  # sim thấp < 0.85 (dựa trên model thực)

def test_is_similar_similar_strings_threshold_met(deduplicator_real):
    result = deduplicator_real.is_similar("giá khoảng 9 tỷ 100 triệu", "giá khoảng 9 tỷ 100 triệu")
    assert result is True  # Giả định sim cao >= 0.85 (kiểm tra thực tế)

# def test_is_similar_similar_strings_threshold_not_met(deduplicator_real):
#     result = deduplicator_real.is_similar("nhà ở Hà Nội", "căn hộ ở Sài Gòn")
#     assert result is False  # sim thấp < 0.85

# def test_is_similar_empty_strings(deduplicator_real):
#     result = deduplicator_real.is_similar("", "")
#     assert result is True  # Hai chuỗi rỗng -> sim = 1.0


# def test_get_price_label_first(deduplicator_real):
#     res = []
#     for price in prices:
#         price_label = deduplicator_real._get_price_label(price)
#         res.append(price_label)
#         print (price_label)
#     result = deduplicator_real.is_similar(res[0], res[1])
#     assert result is True  # sim thấp