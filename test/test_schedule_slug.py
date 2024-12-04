from dags.etl_patient_journey_schedule import parse_schedule_slug
import pytest

# Unit test
def test_parse_schedule_slug():

    assert parse_schedule_slug("4d-2d-pre-op") == (-4, -2, "operation")

    assert parse_schedule_slug("6m-7m-post-op") == (180, 210, "operation")

    assert parse_schedule_slug("2w-post-inv") == (0, 14, "post-inv")

    assert parse_schedule_slug("4d-pre-op") == (-4, 0, "operation")

    assert parse_schedule_slug("1d-pre-3mpo") == (-1, 90, "operation")

    assert parse_schedule_slug("always") == (None, None, "always")
