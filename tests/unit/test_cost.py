"""Cost estimator: sizes, priors, calibration."""

import pytest

from registry_lakehouse.ops import cost


def test_parse_size():
    assert cost.parse_size("9.3GB") == pytest.approx(9.3)
    assert cost.parse_size("512MB") == pytest.approx(0.5)
    assert cost.parse_size("1073741824") == pytest.approx(1.0)
    with pytest.raises(ValueError):
        cost.parse_size("lots")


def test_estimate_scales_with_input_and_format():
    est = cost.estimate({"FR": 10.0, "NO": 10.0})
    fr = sum(e.dbu for e in est if e.country == "FR")
    no = sum(e.dbu for e in est if e.country == "NO")
    assert no > fr  # JSON costs more to parse than CSV
    small = sum(e.dbu for e in cost.estimate({"FR": 1.0}))
    big = sum(e.dbu for e in cost.estimate({"FR": 10.0}))
    assert 5 < big / small < 11


def test_calibration_recovers_known_coefficients(tmp_path):
    hist = tmp_path / "h.csv"
    hist.write_text("stage,input_gb,dbus\nsilver,10,5\nsilver,2,1\nbronze,4,0.8\n")
    coef = cost.calibrate(str(hist))
    assert coef["silver"] == pytest.approx(0.5)
    assert coef["bronze"] == pytest.approx(0.2)


def test_cli_renders_uncalibrated_warning(capsys):
    assert cost.main(["estimate", "--sizes", "FR=9.3GB,GB=2.6GB,NO=200MB", "--usd-per-dbu", "0.5"]) == 0
    out = capsys.readouterr().out
    assert "TOTAL" in out and "UNCALIBRATED" in out


def test_report_sql_targets_system_tables(capsys):
    cost.main(["report-sql"])
    assert "system.billing.usage" in capsys.readouterr().out
