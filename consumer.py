import os
import json
import logging
from dotenv import load_dotenv

from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# ---------------------------------------------------------
# SETUP LOGGING
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Kafka-Mongo-Consumer")

def main():
    # Load biến môi trường từ file .env
    load_dotenv()

    # Cấu hình kết nối Azure Event Hubs (Kafka API)
    EH_NAMESPACE = os.getenv("EH_NAMESPACE")
    TOPIC_NAME = os.getenv("TOPIC_NAME")
    
    # Ở Consumer, ta ưu tiên dùng EH_CONSUMER_CONNECTION_STRING (có quyền Listen).
    # Nếu không trỏ riêng, nó sẽ fallback về EH_CONNECTION_STRING
    CONNECTION_STRING = os.getenv("EH_CONSUMER_CONNECTION_STRING", os.getenv("EH_CONNECTION_STRING"))
    
    # Cần thêm một Group ID cho Consumer (Bắt buộc)
    CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "mongo-inserter-group")

    if not all([EH_NAMESPACE, TOPIC_NAME, CONNECTION_STRING]):
        logger.error("Thiếu cấu hình Event Hubs trong file .env!")
        return

    # Cấu hình chuẩn Kafka -> Azure Event Hubs
    conf = {
        'bootstrap.servers': EH_NAMESPACE,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '$ConnectionString',
        'sasl.password': CONNECTION_STRING,
        'group.id': CONSUMER_GROUP,
        
        # 'earliest': Nếu consumer group chạy lần đầu chưa có mốc nào, nó sẽ đọc từ đầu.
        'auto.offset.reset': 'earliest',
        
        # 'False': Tắt tự động lưu checkpoint offset của Event Hubs. 
        # Ta sẽ tự lưu (commit) bằng code thủ công CỨ MỖI KHI thực sự ghi thành công vào Database.
        'enable.auto.commit': False
    }

    # Cấu hình kết nối MongoDB Atlas
    MONGO_URI = os.getenv("MONGO_URI", "")
    DB_NAME = os.getenv("MONGO_DB", "")
    # Xoá biến cứng MONGO_COLLECTION vì ta sẽ tự động lấy key "source" làm tên Collection

    logger.info("Đang khởi tạo kết nối MongoDB...")
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        
        # Test kết nối nháp 
        mongo_client.server_info() 
        logger.info(f"Đã kết nối MongoDB thành công. DB: {DB_NAME} | Sẽ tự phân luồng Event Hubs vào theo field 'source'")
        
        # Cache những collection đã được thiết lập index (để không phải gọi create_index liên tục)
        active_collections = {}
        
    except Exception as e:
        logger.error(f"Gặp sự cố khi kết nối MongoDB: {e}")
        return

    # =========================================================
    # 3. KHOỞI TẠO & CHẠY CONSUMER
    # =========================================================
    consumer = Consumer(conf)
    
    # Bắt đầu theo dõi (Subscribe) cái Topic
    consumer.subscribe([TOPIC_NAME])
    logger.info(f"Consumer đã subscribe Topic: {TOPIC_NAME}. Đang chờ tin nhắn...")

    # Vòng lặp liên tục để quét message
    try:
        while True:
            # Poll message liên tục với timeout 1 giây (như anh thiết kế)
            msg = consumer.poll(timeout=1.0)
            
            # --- 3A. BỎ QUA NẾU CHƯA CÓ TIN NHẮN MỚI ---
            if msg is None:
                continue

            # --- 3B. KIỂM TRA LỖI NẾU CÓ TRÊN KÊNH TRUYỀN TẢI ---
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Báo hiệu đã đọc tới cuối partition (Tạm hết message), không phải lỗi nghiêm trọng.
                    continue
                else:
                    logger.error(f"Lỗi Consumer cục bộ: {msg.error()}")
                    continue

            # --- 3C. DECODE MESSAGE JSON ---
            try:
                raw_value = msg.value().decode('utf-8')
                record = json.loads(raw_value)
            except Exception as e:
                logger.error(f"Failed to parse JSON từ Kafka: {e}. Raw data: {raw_value}")
                # Kể cả có lỗi format lởm rác bẩn, ta vẫn commit đánh dấu đã đọc để trôi qua tin nhắn này
                consumer.commit(asynchronous=False)
                continue
                
            # --- 3D. VALIDATE FIELD BẮT BUỘC ---
            source_id = record.get('source_id')
            title = record.get('title')
            source_web = record.get('source') # e.g. "raovat321", "batdongsan"
            
            if not all([source_id, title, source_web]):
                logger.warning(f"[SKIPPED] Bỏ qua message do thiếu trường ID, tiêu đề, hoặc tên source_web.")
                # Vẫn commit để trôi qua
                consumer.commit(asynchronous=False)
                continue

            # --- 3E. TỰ ĐỘNG PHÂN LUỒNG MONGODB COLLECTION ---
            if source_web not in active_collections:
                # Nếu lần đầu chưa ghi vào collection này, ta trỏ tới nó và cài Unique Index luôn
                coll = db[source_web]
                coll.create_index("source_id", unique=True)
                active_collections[source_web] = coll
            
            target_collection = active_collections[source_web]

            # --- 3F. INSERT VÀO MONGODB & BẮT LỖI TRÙNG LẶP ---
            try:
                # Ép giá trị '_id' trực tiếp bằng luôn source_id để Database tự check trùng tự nhiên nhất
                # Nếu anh không thích đổi _id gốc mongo thì có thể bỏ dòng này đi (dùng cái create_index nãy là đủ)
                record['_id'] = source_id 
                
                target_collection.insert_one(record)
                logger.info(f"[SUCCESS] Đã chèn Listing {source_id} - '{title[:30]}...' vào Collection '{source_web}'.")
                
                # --- CHỐT HẠ: CHỈ CẬP NHẬT RẰNG "ĐÃ ĐỌC MESSAGE NÀY RỒI" KHI ĐÃ LƯU THÀNH CÔNG ---
                consumer.commit(asynchronous=False)
                
            except DuplicateKeyError:
                logger.warning(f"[DUPLICATE] Bỏ qua tin bị trùng lặp trong MongoDB: {source_id}")
                # Nó bị trùng tức là trong DB đã có, cứ commit để bỏ qua tin nhắn này
                consumer.commit(asynchronous=False)
                
            except Exception as e:
                logger.error(f"[DB ERROR] Lỗi không lường trước khi insert Mongo: {e}")
                # CHÚ Ý: Ở đây KHÔNG gõ consumer.commit(). 
                # Vi nếu mongo bị rớt mạng chập chờn, ta để nguyên message đó chưa commit,
                # lát khởi động lại app nó sẽ tự đọc lại đúng cái message này và thử insert lần nữa!

    # =========================================================
    # 4. GRACEFUL SHUTDOWN (Ctrl + C)
    # =========================================================
    except KeyboardInterrupt:
        logger.info("Phát hiện tín hiệu tắt chương trình (Ctrl+C). Đang dọn dẹp các luồng...")
        
    finally:
        # Ngắt phiên MongoDB
        logger.info("1/2: Đóng kết nối MongoDB...")
        if 'mongo_client' in locals():
            mongo_client.close()
            
        # Tắt Kafka Consumer an toàn
        logger.info("2/2: Giải phóng tiến trình Kafka Consumer...")
        if 'consumer' in locals():
            # Việc gọi consumer.close() CỰC KỲ QUAN TRỌNG:
            # 1. Nó gửi một gói tin 'LeaveGroup' lên Event Hubs báo rẳng tao đã thoát rồi,
            # cho phép phân bổ lại các tin nhắn còn tồn đọng cho một máy khác chạy ngay lập tức.
            # 2. Nó lưu nốt các checkpoint (offset) cuối cùng chưa kịp lưu trước lúc ngắt ngòi.
            consumer.close()
            
        logger.info("Đã tắt an toàn (Graceful Shutdown) toàn bộ!")

if __name__ == "__main__":
    main()