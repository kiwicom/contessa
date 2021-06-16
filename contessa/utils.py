import re
from dataclasses import dataclass
from typing import Any

import jinja2


@dataclass
class AggregatedResult:
    total_records: int
    failed: int
    passed: int
    failed_example: Any = None


def render_jinja_sql(sql, ctx):
    env = jinja2.Environment(
        loader=jinja2.BaseLoader(), undefined=jinja2.StrictUndefined
    )
    t = env.from_string(sql)
    rendered = t.render(**ctx)
    rendered = re.sub(r"%", "%%", rendered)
    return rendered
