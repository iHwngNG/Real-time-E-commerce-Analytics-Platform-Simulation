import os
import sys

# Mở đường dẫn trỏ về gốc dự án
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from simulator.generators.user_generator import UserGenerator
from simulator.generators.product_generator import ProductGenerator
from simulator.generators.event_generator import EventGenerator, load_config
from simulator.producer.kafka_producer import EventProducer

# =============================================================================
# Helper Test Module: Truyền tải data thẳng đến Streaming
# =============================================================================

if __name__ == "__main__":
    print("-" * 60)
    print(" [TESTING] Đang chuẩn bị gói sự kiện giả lập.")
    print("-" * 60)

    cfg = load_config()

    # 1. Khởi tạo ngẫu nhiên một số user/sản phẩm nhỏ
    _ug = UserGenerator()
    _pg = ProductGenerator()
    users_chunk = _ug.generate_batch(50)
    products_chunk = _pg.generate_batch(20)

    # 2. Tạo đối tượng sinh Data Simulator
    event_gen = EventGenerator(users_chunk, products_chunk, cfg)

    # 3. Kết nối thẳng vào Kafka Broker theo port Local/Docker Forward (Kafka Test)
    kafka_servers = "localhost:29092"
    producer = EventProducer(bootstrap_servers=kafka_servers)

    print("[+] Sinh ngẫu nhiên: 50 Bản ghi...")
    # Bơm 50 event ra một lượt
    events = event_gen.generate_batch(50)
    producer.publish_batch(events)

    # Cập nhật Kafka Network buffer
    stats = producer.get_stats()
    producer.close()

    print(f"\n[+] Truyền thông điệp qua Kafka thành công.")
    print(f"[+] Records đã gửi đi : {stats['sent']}")
    if stats["errors"] > 0:
        print(f"[!] Records bị hỏng error: {stats['errors']}")
