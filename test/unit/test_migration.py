from contessa.migration import MigrationsResolver

SQLALCHEMY_URL = "postgresql://postgres:postgres@postgres:5432/test_db"


def test_get_fallback_version_present_in_map():
    versions_migrations = {"0.1.2": "w5rtyuret457", "0.1.3": "dfgdfg5b0ee5"}

    m = MigrationsResolver(versions_migrations, "0.1.3", SQLALCHEMY_URL, "schema")
    fallback_version = m.get_fallback_version()

    assert fallback_version == "0.1.3"


def test_get_fallback_version_less_than_first():
    versions_migrations = {"0.1.2": "w5rtyuret457", "0.1.3": "dfgdfg5b0ee5"}

    m = MigrationsResolver(versions_migrations, "0.1.1", SQLALCHEMY_URL, "schema")
    fallback_version = m.get_fallback_version()

    assert fallback_version == "0.1.2"


def test_get_fallback_version_greather_than_last():
    versions_migrations = {"0.1.2": "w5rtyuret457", "0.1.3": "dfgdfg5b0ee5"}

    m = MigrationsResolver(versions_migrations, "0.1.8", SQLALCHEMY_URL, "schema")
    fallback_version = m.get_fallback_version()

    assert fallback_version == "0.1.3"


def test_get_fallback_version_is_betwen():
    versions_migrations = {
        "0.1.2": "w5rtyuret457",
        "0.1.6": "dfgdfg5b0ee5",
        "0.1.7": "54f8985b0ee5",
        "0.1.9": "480e6618700d",
        "0.2.6": "3w4er8y50yyd",
        "0.2.8": "034hfa8943hr",
    }

    m = MigrationsResolver(versions_migrations, "0.1.8", SQLALCHEMY_URL, "schema")
    fallback_version = m.get_fallback_version()

    assert fallback_version == "0.1.7"

    m = MigrationsResolver(versions_migrations, "0.2.0", SQLALCHEMY_URL, "schema")
    fallback_version = m.get_fallback_version()

    assert fallback_version == "0.1.9"
