import yaml
import argparse
from crawler.vnexpress import VNExpressCrawler  # Import từ crawler.vnexpress


def parse_args():
    parser = argparse.ArgumentParser(description="VNNewsCrawler")
    parser.add_argument("--config_fpath", type=str, default="crawler_config.yml", help="Path to config file")
    return parser.parse_args()


def main(**kwargs):
    config_fpath = kwargs.get("config_fpath", "crawler_config.yml")
    with open(config_fpath, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Khởi tạo crawler với config
    crawler = VNExpressCrawler(**config)

    # Kiểm tra task
    task = config.get("task", "type")
    if task == "urls":
        print("Start crawling urls from urls.txt file...")
        crawler.start_crawling()  # Thu thập từ urls.txt
    elif task == "type":
        # Lấy danh sách danh mục từ config
        article_types = config.get("article_types", [config.get("article_type")])  # Hỗ trợ cả article_type và article_types

        # Đảm bảo article_types là danh sách
        if isinstance(article_types, str):
            article_types = [article_types]

        # Thu thập dữ liệu từ từng danh mục
        for article_type in article_types:
            print(f"Crawling from urls of {article_type}...")
            # Cập nhật config với article_type là chuỗi
            updated_config = config.copy()  # Tạo bản sao của config
            updated_config["article_type"] = article_type  # Đảm bảo article_type là chuỗi
            if "article_types" in updated_config:
                del updated_config["article_types"]  # Xóa article_types để tránh nhầm lẫn

            # Khởi tạo crawler với config đã cập nhật
            crawler = VNExpressCrawler(**updated_config)
            crawler.start_crawling()
    else:
        raise ValueError(f"Unsupported task: {task}. Must be 'urls' or 'type'.")


if __name__ == "__main__":
    args = parse_args()
    main(**vars(args))