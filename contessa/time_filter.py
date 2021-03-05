from datetime import timedelta, datetime
from dataclasses import dataclass
from enum import Enum
from typing import Union, List, Optional, Dict


@dataclass
class TimeFilterColumn:
    column: str
    since: Optional[Union[timedelta, datetime]] = None
    since_inclusive: bool = True
    until: Optional[Union[timedelta, datetime, str]] = None
    until_inclusive: bool = False

    def compose_sql(self, now: datetime):
        result = "("
        if self.since:
            since_str = self.time_to_sql_value(self.since, now)
            result += (
                f"{self.column} >{'=' if self.since_inclusive else ''} {since_str}"
            )
        if self.since and self.until:
            result += " AND "
        if self.until:
            until_str = self.time_to_sql_value(self.until, now)
            result += (
                f"{self.column} <{'=' if self.until_inclusive else ''} {until_str}"
            )
        result += ")"
        return result

    def time_to_sql_value(self, time, now):
        if isinstance(time, str):
            if time == "now":
                time = now
            else:
                raise ValueError("'now' is only allowed string value")
        if isinstance(time, timedelta):
            time = now - time
        return f"'{time.strftime('%Y-%m-%d %H:%M:%S UTC')}'::timestamptz"

    def __str__(self):
        if self.since and self.until:
            return f"{self.column} between {self.since} and {self.until}"
        elif self.since:
            return f"{self.column} > {self.since}"
        elif self.until:
            return f"{self.column} < {self.until}"
        else:
            raise ValueError(
                "Incorrect configuration, at least one of 'since' or 'until' has to be set"
            )


class TimeFilterConjunction(Enum):
    AND = "AND"
    OR = "OR"


@dataclass()
class TimeFilter:
    columns: List[TimeFilterColumn]
    conjunction: TimeFilterConjunction = TimeFilterConjunction.OR
    now: datetime = None

    def __post_init__(self):
        self.now = datetime.now()

    def __str__(self):
        c = f" {self.conjunction.value.lower()} "
        s = c.join(str(c) for c in self.columns)
        if self.now:
            s += f" relative to {self.now}"
        return f"<TimeFilter {s}>"

    @property
    def sql(self):
        sep = f" {self.conjunction.value} "
        return sep.join(c.compose_sql(self.now) for c in self.columns)


# for backwards compatibility with previous versions
def parse_time_filter(time_filter: Union[str, List[Dict], TimeFilter]) -> TimeFilter:
    default_since = timedelta(days=30)

    if time_filter is None or isinstance(time_filter, TimeFilter):
        return time_filter
    if isinstance(time_filter, str):
        return TimeFilter(
            columns=[TimeFilterColumn(time_filter, since=default_since, until="now")]
        )
    if isinstance(time_filter, list):
        return TimeFilter(
            columns=[
                TimeFilterColumn(
                    column["column"],
                    since=timedelta(days=column.get("days", default_since.days)),
                    until="now",
                )
                for column in time_filter
            ]
        )
