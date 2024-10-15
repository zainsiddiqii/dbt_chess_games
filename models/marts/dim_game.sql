with

unique_games as (
  select
    game_id,
    game_url,
    source,
    is_rated,
    date(start_timestamp_utc)   as start_date_utc,
    date(start_datetime_actual) as start_date_actual,
    time(start_timestamp_utc)   as start_time_utc,
    time(start_datetime_actual) as start_time_actual,
    date(end_timestamp_utc)     as end_date_utc,
    date(end_datetime_actual)   as end_date_actual,
    time(end_timestamp_utc)     as end_time_utc,
    time(end_datetime_actual)   as end_time_actual

  from {{ ref("int_game_information") }}
),

dim_game as (
  select
    *,
    generate_uuid() as game_sid

  from unique_games
)

select * from dim_game
