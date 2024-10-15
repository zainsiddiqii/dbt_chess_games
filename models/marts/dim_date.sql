with

dates_series as (
  select *
  from
    unnest(
      generate_date_array('2020-01-01', '2030-01-01', interval 1 day)
    ) as d
),

dim_date as (
  select
    d                         as date_id,
    extract(year from d)      as year,
    extract(week from d)      as year_week,
    extract(day from d)       as month_day,
    extract(dayofyear from d) as year_day,
    format_date('%Q', d)      as quarter,
    extract(month from d)     as month,
    format_date('%B', d)      as month_name,
    format_date('%w', d)      as week_day,
    format_date('%A', d)      as day_name,
    case
      when format_date('%A', d) in ('Sunday', 'Saturday')
        then 0
      else 1
    end                       as day_is_weekday

  from dates_series
)

select * from dim_date
