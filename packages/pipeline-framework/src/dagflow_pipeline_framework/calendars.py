"""Business day calendar generation for pipeline scheduling.

Each calendar defines which days are valid business days for a specific
market or regulatory jurisdiction. The generated days are stored in
control.calendar_days and used by the UI to determine selectable dates.

Supported calendars:
  - us_sec: US SEC/EDGAR filing days (weekdays minus US federal holidays)
  - us_nyse: US NYSE trading days
  - eu_target2: EU TARGET2 settlement days
  - uk_lse: UK London Stock Exchange trading days
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import holidays


CALENDAR_REGISTRY: dict[str, dict[str, Any]] = {
    "us_sec": {
        "label": "US SEC Filing Days",
        "country": "US",
        "description": "Weekdays excluding US federal holidays (SEC/EDGAR schedule)",
    },
    "us_nyse": {
        "label": "US NYSE Trading Days",
        "country": "US",
        "financial": True,
        "description": "NYSE trading calendar",
    },
    "eu_target2": {
        "label": "EU TARGET2 Settlement",
        "country": "EU",
        "description": "European Central Bank TARGET2 settlement days",
    },
    "uk_lse": {
        "label": "UK LSE Trading Days",
        "country": "GB",
        "description": "London Stock Exchange trading calendar",
    },
}


def _get_holidays(calendar_code: str, year: int) -> holidays.HolidayBase:
    config = CALENDAR_REGISTRY.get(calendar_code)
    if config is None:
        raise KeyError(f"Unknown calendar: {calendar_code}")

    country = config["country"]
    if config.get("financial"):
        return holidays.financial_holidays(country, years=year)
    return holidays.country_holidays(country, years=year)


def generate_calendar_days(
    calendar_code: str,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    if calendar_code not in CALENDAR_REGISTRY:
        raise KeyError(f"Unknown calendar: {calendar_code}")

    years = set(range(start_date.year, end_date.year + 1))
    holiday_map: dict[date, str] = {}
    for year in years:
        for d, name in _get_holidays(calendar_code, year).items():
            holiday_map[d] = name

    rows: list[dict[str, Any]] = []
    current = start_date
    while current <= end_date:
        weekday = current.weekday()
        is_weekend = weekday >= 5
        holiday_name = holiday_map.get(current)
        is_holiday = holiday_name is not None

        if is_weekend:
            day_type = "weekend"
        elif is_holiday:
            day_type = "holiday"
        else:
            day_type = "business"

        rows.append({
            "calendar_code": calendar_code,
            "cal_date": current,
            "is_business_day": day_type == "business",
            "day_type": day_type,
            "holiday_name": holiday_name,
        })
        current += timedelta(days=1)

    return rows


def populate_calendar(
    dsn: str,
    calendar_code: str,
    start_date: date,
    end_date: date,
) -> int:
    import psycopg

    rows = generate_calendar_days(calendar_code, start_date, end_date)

    query = """
    insert into control.calendar_days (calendar_code, cal_date, is_business_day, day_type, holiday_name)
    values (%(calendar_code)s, %(cal_date)s, %(is_business_day)s, %(day_type)s, %(holiday_name)s)
    on conflict (calendar_code, cal_date) do update
    set is_business_day = excluded.is_business_day,
        day_type = excluded.day_type,
        holiday_name = excluded.holiday_name
    """

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(query, row)
        conn.commit()

    return len(rows)


def available_calendars() -> list[dict[str, str]]:
    return [
        {"code": code, "label": config["label"], "description": config["description"]}
        for code, config in CALENDAR_REGISTRY.items()
    ]
