import logging
from typing import List, Text, Type

from datadog.dogstatsd.base import DEFAULT_HOST, DEFAULT_PORT

from contessa.db import Connector
from contessa.models import DQBase, QualityCheck
from datadog import DogStatsd


class Destination:
    def __init__(self):
        pass

    def ensure_destination(self, dq_class: Type[DQBase]) -> None:
        pass

    def persist(self, items: List[DQBase]) -> None:
        pass


class DBDestination(Destination):
    def __init__(self, conn_uri_or_engine: Text):
        super().__init__()
        self.conn_uri_or_engine = conn_uri_or_engine
        self.conn = Connector(conn_uri_or_engine)

    def ensure_destination(self, quality_check_class: Type[DQBase]) -> None:
        self.conn.ensure_table(quality_check_class.__table__)

    def persist(self, items: List[DQBase]) -> None:
        self.conn.upsert(items)


class StatsDDestination(Destination):
    def __init__(
        self,
        statsd_prefix: Text,
        statsd_host: Text = DEFAULT_HOST,
        statsd_port: int = DEFAULT_PORT,
    ):
        super().__init__()
        self.statsd = DogStatsd(host=statsd_host, port=statsd_port)
        self.statsd_prefix = statsd_prefix.rstrip(".")

    def ensure_destination(self, quality_check_class: Type[DQBase]) -> None:
        pass

    def persist(self, items: List[DQBase]) -> None:
        for item in items:
            if isinstance(item, QualityCheck):
                self.persist_quality_check(item)
            else:
                logging.warning(
                    f"Not persisted. Reason: no handler for {type(item)} type. Skipping..."
                )

    def persist_quality_check(self, item: QualityCheck) -> None:
        tags = [
            f"rule_name:{item.rule_name}",
            f"rule_type:{item.rule_type}",
            f"attribute:{item.attribute}",
        ]

        self.statsd.increment(
            f"{self.statsd_prefix}.total_records", item.total_records, tags=tags,
        )

        self.statsd.increment(
            f"{self.statsd_prefix}.failed_records", item.failed, tags=tags,
        )

        self.statsd.increment(
            f"{self.statsd_prefix}.passed_records", item.passed, tags=tags,
        )

        self.statsd.gauge(
            f"{self.statsd_prefix}.failed_records_percentage",
            item.failed_percentage,
            tags=tags,
        )

        self.statsd.gauge(
            f"{self.statsd_prefix}.passed_records_percentage",
            item.passed_percentage,
            tags=tags,
        )
