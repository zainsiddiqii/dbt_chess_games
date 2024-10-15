with

results as (
  select distinct
    my_result as result,
    result_method

  from {{ ref("int_game_information") }}

  union all

  select distinct
    opponent_result as result,
    result_method

  from {{ ref("int_game_information") }}
),

unique_results as (
  select distinct
    result,
    result_method

  from results
),

dim_result as (
  select
    *,
    generate_uuid() as result_sid

  from unique_results
)

select * from dim_result
