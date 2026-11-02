with date_spine as (
    select generate_series(
        '2026-01-01'::date, 
        '2029-12-31'::date,
        '1 day'::interval
    )::date as date_day
),

final as (

    select

        date_day,
        extract(year from date_day)::int                as year,
        extract(quarter from date_day)::int             as quarter,
        extract(month from date_day)::int               as month,
        to_char(date_day, 'FMMonth')                    as month_name, --FILL MODE

        extract(isodow from date_day)::int              as day_of_week_num,     
        to_char(date_day, 'FMDay')                      as day_of_week_name, --FILL MODE

        case when extract(isodow from date_day) in (6,7)
            then true else false end                    as is_weekend

        from date_spine
)

select * from final