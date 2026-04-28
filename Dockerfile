# Sử dụng image Python làm base (có thể cài thêm Chrome/Chromium)
FROM python:3.10-slim

# Ngăn Python giữ buffer file log (in thẳng ra terminal)
ENV PYTHONUNBUFFERED=1

# Tạo và chuyển tới thư mục làm việc trong container
WORKDIR /app

# Cài đặt các công cụ hệ thống và Chromium cần thiết cho web scraping bằng Selenium
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    chromium \
    chromium-driver \
    librdkafka-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt các thư viện Python
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ source code vào container
COPY . /app/

# Câu lệnh mặc định khi container chạy (ví dụ chạy scraper)
# Lệnh dưới đây có thể thay đổi tuỳ vào cấu trúc code, như "source all"
CMD ["python", "main.py", "--source", "all"]