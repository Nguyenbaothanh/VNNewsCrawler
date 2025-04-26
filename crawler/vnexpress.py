import requests
import sys
from pathlib import Path
import psycopg2
from datetime import datetime
import time

from bs4 import BeautifulSoup

FILE = Path(__file__).resolve()
ROOT = FILE.parents[1]  # root directory
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))  # add ROOT to PATH

from logger import log
from crawler.base_crawler import BaseCrawler
from utils.bs4_utils import get_text_from_tag


class VNExpressCrawler(BaseCrawler):

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.logger = log.get_logger(name=__name__)
        self.article_type_dict = {
            0: "thoi-su",
            1: "du-lich",
            2: "the-gioi",
            3: "kinh-doanh",
            4: "khoa-hoc",
            5: "giai-tri",
            6: "the-thao",
            7: "phap-luat",
            8: "giao-duc",
            9: "suc-khoe",
            10: "doi-song"
        }
        # Initialize PostgreSQL database connection
        try:
            self.conn = psycopg2.connect(
                host="khoadue.me",
                port=5434,
                database="blog_automation_db",
                user="postgres",
                password="admin123"
            )
            self.create_table()
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def create_table(self):
        """Create a table named 'newtable' to store articles if it doesn't exist"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS newtable (
                    url TEXT PRIMARY KEY,
                    title TEXT,
                    post_time TEXT,
                    categories TEXT
                )
            ''')
            # Ensure the categories column exists in existing table
            cursor.execute('''
                ALTER TABLE newtable
                ADD COLUMN IF NOT EXISTS categories TEXT
            ''')
            self.conn.commit()
        except Exception as e:
            self.logger.error(f"Error creating or updating table: {e}")
            raise

    def extract_content(self, url: str) -> tuple:
        """
        Extract title and post time (date only) from url
        @param url (str): url to crawl
        @return title (str)
        @return post_time (str): Date in format DD/MM/YYYY
        """
        content = requests.get(url).content
        soup = BeautifulSoup(content, "html.parser")

        # Extract title
        title = soup.find("h1", class_="title-detail") 
        if title is None:
            return None, None
        title = title.text.strip()

        # Try to extract post time from <meta> tag with itemprop="datePublished"
        post_time_meta = soup.find("meta", itemprop="datePublished")
        if post_time_meta and post_time_meta.get("content"):
            # Example content: "2025-04-27T08:00:00+07:00"
            post_time_str = post_time_meta["content"]
            try:
                post_time_dt = datetime.fromisoformat(post_time_str.replace("Z", "+00:00"))
                post_time = post_time_dt.strftime("%d/%m/%Y")  # Convert to DD/MM/YYYY
            except ValueError:
                post_time = "N/A"
        else:
            # Fallback: Check other possible tags like <span class="date"> or <time>
            post_time_tag = soup.find("span", class_="date")
            post_time_full = post_time_tag.text.strip() if post_time_tag else None
            if post_time_full and "," in post_time_full:
                # If format is "Thứ 2, 27/04/2025, 08:00", take the date part
                post_time = post_time_full.split(",")[1].strip()
            else:
                post_time = "N/A"

        return title, post_time

    def write_content(self, url: str, output_fpath: str, category: str = None) -> bool:
        """
        From url, extract title and post time then save to PostgreSQL database (newtable) along with url and category
        If URL already exists, update the title, post_time, and category
        @param url (str): url to crawl
        @param output_fpath (str): not used (kept for compatibility)
        @param category (str): category of the article (e.g., du-lich)
        @return (bool): True if save or update successfully, False otherwise
        """
        title, post_time = self.extract_content(url)
                
        if title is None:
            return False

        # If category is not provided, use self.article_type
        category = category or self.article_type

        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO newtable (url, title, post_time, categories)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (url) DO UPDATE
                SET title = EXCLUDED.title,
                    post_time = EXCLUDED.post_time,
                    categories = EXCLUDED.categories
            ''', (url, title, post_time, category))
            self.conn.commit()
            return True
        except Exception as e:
            self.logger.error(f"Error saving to database: {e}")
            return False

    def get_urls_of_type_thread(self, article_type, page_number):
        """Get urls of articles in a specific type in a page"""
        page_url = f"https://vnexpress.net/{article_type}-p{page_number}"
        content = requests.get(page_url).content
        time.sleep(1)  # Thêm thời gian nghỉ 1 giây để tránh bị chặn
        soup = BeautifulSoup(content, "html.parser")
        titles = soup.find_all(class_="title-news")

        if len(titles) == 0:
            self.logger.info(f"Couldn't find any news in {page_url} \nMaybe you sent too many requests, try using less workers")

        articles_urls = list()

        for title in titles:
            link = title.find_all("a")[0]
            url = link.get("href")
            articles_urls.append(url)
    
        return articles_urls

    def __del__(self):
        """Close the database connection when the object is destroyed"""
        if hasattr(self, 'conn'):
            self.conn.close()