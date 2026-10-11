from decimal import Decimal

import pytest

from ticklab.model import to_decimal


def test_decimal_text_is_kept_exactly():
    assert to_decimal("83040.01000000") == Decimal("83040.01")
    assert to_decimal("0.00013000") == Decimal("0.00013")
    assert str(to_decimal("0.1")) == "0.1"  # no binary rounding


@pytest.mark.parametrize(
    "text",
    [
        "0",
        "-1.5",
        "NaN",
        "Infinity",
        "0.0000000000001",  # 13 decimal places: more than the schema's scale of 12
        "12345678901234567",  # 17 digits before the point: more than 28 - 12
    ],
)
def test_values_the_schema_cannot_hold_are_rejected(text):
    with pytest.raises(ValueError):
        to_decimal(text)
