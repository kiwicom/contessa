from test.conftest import FakedDatetime

from contessa.models import get_default_qc_class


# Start ignoring RadonBear
def test_quality_check_init_row(rule, results, context, hook):
    qc = get_default_qc_class("booking")
    assert qc.__tablename__ == "quality_check_booking"

    qc.__table__.create(hook.get_sqlalchemy_engine())
    instance = qc()
    instance.init_row(rule, results, context)

    assert instance.task_ts == context["ts"]
    assert instance.attribute == "src"
    assert instance.rule_name == "not_null"
    assert instance.rule_description == "True when data is null."
    assert instance.total_records == 5
    assert instance.failed == 2
    assert instance.passed == 3
    assert instance.failed_percentage == 40
    assert instance.passed_percentage == 60
    assert instance.median_30_day_failed is None
    assert instance.median_30_day_passed is None
    assert instance.time_filter is None
    assert instance.status == "invalid"


# Stop ignoring


def test_set_medians(hook, monkeypatch):
    qc = get_default_qc_class("t")
    qc.__table__.create(hook.get_sqlalchemy_engine())
    instance = qc()

    hook.run(
        """
        insert into data_quality.quality_check_t(failed, passed, task_ts)
        values
          (10, 200, '2018-09-11T13:00:00'),
          (3, 22, '2018-09-10T13:00:00'),
          (11, 110, '2018-09-09T13:00:00'),
          (55, 476, '2018-09-08T13:00:00'),
          (77, 309, '2018-07-12T13:00:00') -- should not be taken
    """
    )

    monkeypatch.setattr("contessa.models.datetime", FakedDatetime)
    instance.set_medains()

    assert instance.median_30_day_failed == 10.5
    assert instance.median_30_day_passed == 155
