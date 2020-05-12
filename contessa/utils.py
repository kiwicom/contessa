from datetime import timedelta

from contessa.settings import TIME_FILTER_DEFAULT


def compose_where_time_filter(time_filter, task_ts):
    days = 30
    filters = []
    result = []
    if time_filter != TIME_FILTER_DEFAULT:
        if isinstance(time_filter, str):
            filters.append({"column": time_filter, "days": days})
        elif isinstance(time_filter, list):
            for each in time_filter:
                filters.append(
                    {"column": each["column"], "days": each.get("days", days)}
                )

        for each in filters:
            present = task_ts.strftime("%Y-%m-%d %H:%M:%S UTC")
            past = (task_ts - timedelta(days=each["days"])).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
            result.append(
                f"""({each["column"]} >= '{past}'::timestamptz AND {each["column"]} < '{present}'::timestamptz)"""
            )

    return " OR ".join(result)
