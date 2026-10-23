import requests
import json

def get_latest_article() -> dict:
    """
    Lấy dữ liệu từ wiki theo thời gian thực.
    Đầu ra:
        Json có dạng:
            article_id:
            created_date:
            updated_date:
            title:
            content:
        
    Note: Sau khi cleaned thì phải có thể "path" (để chỉ đường dẫn vào parquet).
    """
    url = "https://en.wikipedia.org/w/api.php"

    headers = {
        # Viết dòng này để tránh 403. Có thể thay bằng email thật. Mà để vậy chạy cũng bth.
        "User-Agent": "MyWikiFetcher/1.0 (your_email@example.com)"
    }

    def query(params: dict) -> dict:
        response = requests.get(
            url,
            headers=headers,
            params={
                "action": "query",
                "format": "json",
                "formatversion": 2,
                **params,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            raise RuntimeError(data["error"])

        return data["query"]

    # Lấy bài mới tạo, thuộc namespace bài viết, bỏ trang chuyển hướng.
    data = query({
        "list": "recentchanges",
        "rctype": "new",
        "rcnamespace": 0,
        "rcshow": "!redirect",
        "rcprop": "ids|title|timestamp",
        "rcdir": "older",
        "rclimit": 1,
    })

    changes = data["recentchanges"]
    if not changes:
        return {}

    article_id = changes[0]["pageid"]

    # Lấy nội dung văn bản và thời điểm sửa đổi gần nhất.
    data = query({
        "pageids": article_id,
        "prop": "extracts|revisions",
        "explaintext": 1,
        "rvprop": "timestamp",
        "rvlimit": 1,
        "rvdir": "older",
    })

    page = data["pages"][0]
    if page.get("missing", False):
        return {}

    # Lấy thời điểm của phiên bản đầu tiên.
    first_revision = query({
        "pageids": article_id,
        "prop": "revisions",
        "rvprop": "timestamp",
        "rvlimit": 1,
        "rvdir": "newer",
    })

    return {
        "article_id": article_id,
        "created_date": first_revision["pages"][0]["revisions"][0]["timestamp"],
        "updated_date": page["revisions"][0]["timestamp"],
        "title": page["title"],
        "content": page.get("extract", ""),
        "path": None,
    }

    return {}
    

