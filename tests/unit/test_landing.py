"""Landing listing: a missing folder is "no files", however and whenever Spark reports it."""

import pytest

from registry_lakehouse.bronze.landing import list_landing_files

pytestmark = pytest.mark.spark


class LazySpark:
    """Mimics Spark Connect (serverless): load() succeeds, the missing path fails at collect()."""

    def __init__(self, message):
        self.message = message

    @property
    def read(self):
        return self

    def format(self, *_):
        return self

    def option(self, *_):
        return self

    def load(self, *_):
        return self

    def select(self, *_):
        return self

    def where(self, *_):
        return self

    def collect(self):
        raise RuntimeError(self.message)


def test_missing_folder_reported_lazily_means_no_files(spark):  # session needed to build F.col
    spark = LazySpark("[PATH_NOT_FOUND] Path does not exist: dbfs:/Volumes/registry/files/landing/_GEOCODE/results.")
    assert list_landing_files(spark, "/Volumes/x", "_GEOCODE/results") == []


def test_other_errors_are_not_swallowed(spark):
    with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
        list_landing_files(LazySpark("PERMISSION_DENIED: no READ VOLUME"), "/Volumes/x", "FRANCE")
