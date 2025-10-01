import json
from utils import retry
from utils import query_json
import pathlib


@retry(max_attempts=5, delay=5)
def fetch_cn_company(data_path:pathlib.Path):
    fetch = query_json("cn/company", {"includeDelisted":True})
    if fetch['message'] != "success":
        raise Exception
    index_data = fetch["data"]
    with open(data_path.joinpath("cn_company.json"), "w",encoding="utf-8") as f:
        json.dump(index_data, f, indent=4, ensure_ascii=False)
