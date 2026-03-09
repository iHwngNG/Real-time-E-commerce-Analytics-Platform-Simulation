import os
import sys

# Thêm đường dẫn gốc để import từ thư mục cha
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from spark_session import create_spark_session
from jobs.ingest_stream import read_raw_events, process_ingestion

# =============================================================================
# Stream Ingestion TEST SUITE
# Purpose: Kiểm thử pipeline đọc Event JSON qua Kafka, bóc tách Schema,
# Flatten fields rồi IN TRỰC TIẾP RA CONSOLE (Bằng PySpark Structured Streaming).
# =============================================================================


def run_test():
    # 1. Gán biến môi trường Kafka cho Docker/Local testing
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "kafka:9092"

    print("=" * 60)
    print(" [TESTING] Khởi động PySpark Pipeline: Stream Ingestion")
    print("=" * 60)

    # 2. Tạo Spark Session
    spark = create_spark_session(app_name="TestIngestionPipeline")

    # 3. Kết nối luồng đọc từ Kafka
    df_raw = read_raw_events(spark)

    # 4. Gắn Schema, Bóc Flatten, Phân rã dữ liệu hợp lệ/Lỗi
    clean_df, dead_letter_df = process_ingestion(df_raw)

    print("\n[+] Đang lắng nghe luồng sự kiện mới...")
    print("[+] Nếu bạn không thấy Data, hãy chạy scripts tạo Event tại Simulator.")

    # 5. Pipeline này được ghim chạy liên tục (Console output)
    # Bỏ truncate=False để in gọn dữ liệu lên một dòng dài
    query = (
        clean_df.writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run_test()
