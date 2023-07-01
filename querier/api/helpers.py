from datetime import datetime


def parse_date(date: str) -> datetime:
    return datetime.strptime(date, "%Y-%m-%d %H:%M:%S")


def calculate_granurality(start_date, end_date) -> int:
    start_time = parse_date(start_date)
    end_time = parse_date(end_date)

    duration = (end_time - start_time).total_seconds()

    max_data_points = 600  # 10 mintues
    minimum_granurality = 1  # till 19.9 miutes data will be group per second

    granurality_in_seconds = duration // max_data_points

    return max(granurality_in_seconds, minimum_granurality)
