import logging
import re
from typing import List, Dict, Optional
from utils.data_validator import PropertyDataValidator
from bson import ObjectId
from datetime import datetime
from dateutil import parser as date_parser
from sentence_transformers import SentenceTransformer, util  # pip install sentence-transformers
import torch  # Để tính cosine sim

logger = logging.getLogger(__name__)

class Deduplicator:
    def __init__(self):
        """
        Deduplicator chịu trách nhiệm xác định các listing mới, có thể cập nhật, hoặc trùng lặp
        dựa trên source_id và nội dung.
        """
        self.SIMILARITY_THRESHOLD = 0.93
        self.CROSS_SOURCE_THRESHOLD = 0.90
        self.PRICE_DIFF_THRESHOLD = 0.15
        self.QUERY_LIMIT = 1000
        self.embedder = SentenceTransformer('distiluse-base-multilingual-cased-v1')  

    # --- HÀM IS_SIMILAR ĐÃ ĐƯỢC TÁI CẤU TRÚC ---
    def is_similar(
        self,
        new_listing_embedding: torch.Tensor,
        new_listing_str: str, # Chuỗi của tin mới để debug
        candidate_listing: Dict[str, any]
    ) -> bool:
        """
        Kiểm tra tương đồng giữa một embedding của tin mới và một tin ứng viên (dictionary).
        Hàm này hiệu quả hơn vì nó không encode lại tin mới mỗi lần gọi.
        """
        # 1. Tạo chuỗi so sánh cho ứng viên từ DB
        candidate_str = self._create_comparison_string(candidate_listing)
        if not candidate_str:
            return False

        # 2. Encode chuỗi của ứng viên
        candidate_embedding = self.embedder.encode(
            candidate_str, 
            convert_to_tensor=True, 
            normalize_embeddings=True
        )

        # 3. Tính toán cosine similarity
        cosine_sim = torch.cosine_similarity(
            new_listing_embedding.unsqueeze(0),
            candidate_embedding.unsqueeze(0)
        ).item()

        # 4. In thông tin debug (đã sửa lại cho đúng)
        sim_percent = cosine_sim * 100
        threshold_percent = self.SIMILARITY_THRESHOLD * 100
        logger.debug(
            f"Similarity: {sim_percent:.2f}% (Threshold: {threshold_percent:.1f}%)\n"
            f"  - New:      '{new_listing_str[:80]}...'\n"
            f"  - Candidate:'{candidate_str[:80]}...'"
        )
        
        return cosine_sim >= self.SIMILARITY_THRESHOLD

    # --- HÀM FIND_DUPLICATE_CANDIDATE ĐƯỢC ĐƠN GIẢN HÓA ---
    def find_duplicate_candidate(
        self,
        new_listing: Dict[str, any],
        db_client,
        source: str
    ) -> Optional[Dict]:
        """
        Tìm dữ liệu trùng lặp trong DB cho một tin đăng mới bằng cách sử dụng is_similar.
        """
        collection = db_client.get_collection(source)

        # 1. Tạo chuỗi và embedding cho tin mới MỘT LẦN DUY NHẤT
        new_listing_str = self._create_comparison_string(new_listing)
        if not new_listing_str:
            return None # Không có gì để so sánh

        new_listing_embedding = self.embedder.encode(
            new_listing_str, 
            convert_to_tensor=True, 
            normalize_embeddings=True
        )

        # 2. Lấy các ứng viên từ DB
        # GHI CHÚ: Ở đây ta lấy toàn bộ document vì is_similar cần dictionary đầy đủ
        candidates_cursor = collection.find({}).sort("crawled_at", -1).limit(200)

        # 3. Lặp và gọi is_similar
        for candidate in candidates_cursor:
            # Bỏ qua việc so sánh một tin với chính nó (nếu có thể xảy ra)
            if (candidate.get('source') == new_listing.get('source') and 
                candidate.get('source_id') == new_listing.get('source_id')):
                continue

            if self.is_similar(new_listing_embedding, new_listing_str, candidate):
                # Tìm thấy tin trùng đầu tiên, trả về ngay lập tức
                logger.info(
                    f"Duplicate found for '{new_listing.get('title', '')[:30]}...'. "
                    f"Similar to candidate: '{candidate.get('source')}/{candidate.get('source_id')}'"
                )
                return candidate
        
        # 4. Nếu không tìm thấy tin nào trùng sau khi duyệt hết
        return None

    def _get_price_label(self, price: dict) -> str:
        """Trả về nhãn giá dựa trên giá trị (phiên bản đã sửa lỗi)"""
        if not isinstance(price, dict):
            raise TypeError("Input 'price' must be a dictionary.")
        
        amount = price.get('amount', 0)
        unit = price.get('unit', 'total').lower()

        if not isinstance(amount, (int, float)):
            raise TypeError("Price 'amount' must be a number.")

        # SỬA LỖI CHÍ MẠNG Ở ĐÂY
        if unit == 'million': 
            amount = amount * 1000000  # 1e6
        elif unit == 'billion':
            amount = amount * 1000000000 # 1e9

        # Phân loại giá
        if amount < 5e8:
            return "Dưới 500 triệu"
        elif amount < 1e9:
            return "Khoảng 500 triệu đến 1 tỷ"
        elif amount < 2e9:
            return "Khoảng 1 đến 2 tỷ"
        elif amount < 3e9:
            return "Khoảng 2 đến 3 tỷ"
        elif amount < 4e9:
            return "Khoảng 3 đến 4 tỷ"
        elif amount < 5e9:
            return "Khoảng 4 đến 5 tỷ"
        elif amount < 6e9:
            return "Khoảng 5 đến 6 tỷ"
        elif amount < 7e9:
            return "Khoảng 6 đến 7 tỷ"
        elif amount < 8e9:
            return "Khoảng 7 đến 8 tỷ"
        elif amount < 20e9:
            return "Khoảng 15 đến 20 tỷ"
        elif amount < 30e9:
            return "Khoảng 20 đến 30 tỷ"
        else:
            return "Trên 30 tỷ"
        
    def _normalize_price_unit (self, price_first: Dict, price_second: Dict ) -> List[str]:
        result  = []
        amount_first = price_first.get('amount', 0)
        unit_first = price_first.get('unit', 'total').lower()

        amount_second = price_second.get('amount', 0)
        unit_second = price_second.get('unit', 'total').lower()

        if unit_first == unit_second:
            if unit_first == 'million':
                if abs(amount_first - amount_second) < 1e8:
                    result.append(f"Giá khoảng {amount_first / 1e6:.1f} triệu")
                else:
                    result.append(f"{amount_first}")
                    result.append(f"{amount_second}")
            
            if unit_first == 'billion':
                if abs(amount_first - amount_second) < 1e9 + 1e8:
                    result.append(f"Giá khoảng {amount_first / 1e9:.1f} tỷ")
                else:
                    result.append(f"{amount_first}")
                    result.append(f"{amount_second}")
        else:
            list_first = str(price_first.get('original_text') or price_first.get('amount') or '').split()
            list_second = str(price_second.get('original_text') or price_second.get('amount') or '').split()
            
            result.append(f"Giá khoảng {list_first}")
            result.append(f"Giá khoảng {list_second}")
            
        return result

    def _create_comparison_string(self, listing: Dict[str, any]) -> str:
        """
        Tạo một chuỗi chuẩn hóa từ các trường chính của listing để so sánh sự tương đồng.
        Hàm này được thiết kế để xử lý linh hoạt các cấu trúc dữ liệu khác nhau.
        """
        # === BƯỚC 1: Lấy các giá trị thô ra một cách an toàn ===
        
        source = listing.get('source', '')
        title = listing.get('title', '')
        
        # Lấy 'categories', đảm bảo nó là một danh sách
        categories = listing.get('categories', [])
        if not isinstance(categories, list):
            categories = [] # Nếu categories không phải list, coi như nó rỗng

        # Lấy 'location'
        location_data = listing.get('location')

        
        # === BƯỚC 2: Xử lý và chuẩn hóa từng phần tử ===

        # Chuẩn hóa 'categories' thành một chuỗi
        category_str = ' > '.join(categories) if categories else ''
        
        # Chuẩn hóa 'location' (phần quan trọng nhất)
        location_str_part = ''
        if isinstance(location_data, dict):
            # Trường hợp location là dictionary {'district': ..., 'city': ...}
            district = location_data.get('district', '')
            city = location_data.get('city', '')
            # Dùng filter(None, ...) để loại bỏ các chuỗi rỗng trước khi join
            location_parts = filter(None, [district, city])
            location_str_part = ', '.join(location_parts)
        elif isinstance(location_data, str):
            # Trường hợp location là một chuỗi '· Thanh Trì, Hà Nội'
            location_str_part = location_data.strip('· ')

            
        # === BƯỚC 3: Ghép các phần đã chuẩn hóa thành chuỗi so sánh cuối cùng ===
        
        # Sử dụng một danh sách để xây dựng chuỗi, đây là cách làm hiệu quả và an toàn
        comparison_parts = []
        
        # Chỉ thêm vào danh sách nếu giá trị đó thực sự tồn tại (không rỗng)
        if source:
            comparison_parts.append(f"Nguồn {source}:")
        if title:
            comparison_parts.append(title.strip()) # Thêm strip() để chắc chắn
        if category_str:
            comparison_parts.append(f"Loại {category_str}")
        if location_str_part:
            comparison_parts.append(f"Ở {location_str_part}")

        # Nối tất cả các phần tử trong danh sách lại với nhau bằng một khoảng trắng
        return ' '.join(comparison_parts)
        
    def normalize_listing(self, listing: Dict) -> Dict:
        """Chuẩn hóa listing, tận dụng Validator để xác thực và làm sạch trước."""


    def calculate_similarity(self, listing1: Dict, listing2: Dict) -> float:
        """Tính độ tương đồng dùng embedding, tránh lỗi nếu thiếu description."""



    def detect_similar_in_source(self, source: str, new_listing: Dict) -> Optional[str]:
        """" Phát hiện trùng lặp trong cùng 1 nguồn"""

    def detect_cross_source_duplicate(self, new_listing: Dict) -> Optional[Dict]:
        """ Phát hiện trùng lặp giữa các nguồn khác nhau"""

    def merge_listing_data(self, existing: Dict, new_listing: Dict) -> Dict:
        """Xử lý dữ liệu trùng lặp"""

    def classify_listings(self, source: str, listings: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Phân loại listings thành mới, cập nhật, và trùng lặp dựa trên source_id và nội dung.
        """

    def _needs_update(self, existing: Dict, incoming: Dict) -> bool:
        """
        Kiểm tra xem listing có cần cập nhật dựa trên độ tương đồng hoặc thời gian.
        """

    def _check_cross_source_duplicate(self, listing: Dict) -> Optional[Dict]:
        """
        Kiểm tra trùng lặp khác nguồn (gọi detect_cross_source_duplicate).
        """

