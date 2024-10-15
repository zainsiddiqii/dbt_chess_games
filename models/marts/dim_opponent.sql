with

unique_opponents as (
  select distinct
    opponent_id,
    opponent_username    as username,
    opponent_country     as country,
    opponent_status      as status,
    opponent_is_verified as is_verified

  from {{ ref("int_game_information") }}
),

dim_opponent as (
  select
    *,
    generate_uuid() as opponent_sid

  from unique_opponents
)

select * from dim_opponent
