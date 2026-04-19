create table if not exists control.calendar_days (
    calendar_code text not null,
    cal_date date not null,
    is_business_day boolean not null,
    day_type text not null,
    holiday_name text,
    primary key (calendar_code, cal_date)
);

create index if not exists idx_calendar_days_date on control.calendar_days (cal_date, calendar_code);
create index if not exists idx_calendar_days_business on control.calendar_days (calendar_code, is_business_day, cal_date);
