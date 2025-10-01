import time
import logging
from datetime import timedelta
from functools import wraps
from typing import List, Dict, Any, Optional
from .lixinger import query_json

logging.basicConfig(level=logging.INFO)


def retry(max_attempts=3, delay=1):
    """
    装饰器：在函数执行失败时自动重试。

    :param max_attempts: 最大重试次数
    :param delay: 每次重试之间的延迟（秒）
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    logging.info(f"第 {attempts} 次尝试失败: {e}")
                    if attempts < max_attempts:
                        time.sleep(delay)
            logging.info("所有尝试均失败，抛出最后的异常。")
            raise e  # 抛出最后一次异常

        return wrapper

    return decorator


def find_dict_by_field(
        dict_list: List[Dict[str, Any]],
        field_name: str,
        field_value: Any
) -> Optional[Dict[str, Any]]:
    """
    在字典列表中查找指定字段具有给定值的第一个字典。

    该函数遍历输入的字典列表，检查每个字典中指定字段的值是否等于目标值。
    返回第一个匹配的字典，如果没有找到匹配项则返回None。

    Args:
        dict_list: 包含字典的列表，每个字典代表一个数据记录
        field_name: 要匹配的字段名称（字典的键）
        field_value: 期望的字段值，用于匹配比较

    Returns:
        Optional[Dict[str, Any]]: 返回第一个匹配的字典，如果没有找到则返回None
    """
    # 验证输入参数
    if not isinstance(dict_list, list):
        raise TypeError(f"Expected list, got {type(dict_list).__name__}")

    # 遍历字典列表查找匹配项
    for item in dict_list:
        # 确保列表中的每个元素都是字典
        if not isinstance(item, dict):
            continue  # 跳过非字典元素

        # 检查指定字段是否存在且值匹配
        if field_name in item and item[field_name] == field_value:
            return item

    # 没有找到匹配项
    return None


def get_dates_ranges(launch_datetime, end_datetime, years=5):
    """
    根据开始日期和发射日期计算日期范围列表。

    参数:
    start_datetime: datetime对象，表示开始日期。
    launch_datetime: datetime对象，表示发射日期。

    返回:
    一个包含日期范围元组的列表，每个元组包含两个格式为"%Y-%m-%d"的字符串，分别表示范围的开始和结束日期。

    抛出:
    ValueError: 如果发射日期晚于开始日期，则抛出此异常。
    """
    # 检查发射日期是否早于开始日期，如果不是，则抛出ValueError
    if launch_datetime > end_datetime:
        raise ValueError("Launch date must be before start date")

    date_ranges = []
    # 当发射日期小于开始日期时，循环继续
    while launch_datetime < end_datetime:
        # 计算结束日期，确保不超过开始日期，且最多增加10年
        _end_datetime = min(launch_datetime + timedelta(days=365 * years), end_datetime)
        # 将开始日期和结束日期格式化为字符串，并添加到日期范围列表中
        date_ranges.append((launch_datetime.strftime("%Y-%m-%d"), _end_datetime.strftime("%Y-%m-%d")))
        # 将启示期更新为结束日期下一天，为下一次循环做准备
        launch_datetime = _end_datetime + timedelta(days=1)

    return date_ranges
