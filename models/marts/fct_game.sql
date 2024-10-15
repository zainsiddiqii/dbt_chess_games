with

columns_to_join_on as (
  select
    game_id,
    opening_name,
    opponent_id,
    my_result,
    result_method,
    colour,
    time_class,
    time_control

  from {{ ref("int_game_information") }}
),

joining_dims as (
  select
    cols.game_id,
    dim_game.game_sid,
    dim_opening.opening_sid,
    dim_opponent.opponent_sid,
    dim_result.result_sid,
    dim_time_control.time_control_sid

  from columns_to_join_on as cols

  left join {{ ref("dim_game") }} as dim_game
    on cols.game_id = dim_game.game_id

  left join {{ ref("dim_opening") }} as dim_opening
    on dim_opening.opening_name
    = trim(split(cols.opening_name, ':')[safe_offset(0)])
    and coalesce(dim_opening.opening_variation, '')
    = coalesce(trim(split(cols.opening_name, ':')[safe_offset(1)]), '')

  left join {{ ref("dim_opponent") }} as dim_opponent
    on cols.opponent_id = dim_opponent.opponent_id

  left join {{ ref("dim_result") }} as dim_result
    on cols.my_result = dim_result.result
    and cols.result_method = dim_result.result_method

  left join {{ ref("dim_time_control") }} as dim_time_control
    on cols.time_control = dim_time_control.time_control
),

fact_columns as (
  select
    game_id,
    my_rating,
    opponent_rating,
    my_accuracy,
    opponent_accuracy,
    move_number_reached,
    total_moves,
    datetime_diff(
      end_timestamp_utc,
      start_timestamp_utc,
      second
    )             as game_length,
    lead(my_rating, 1) over (
      partition by source
      order by start_timestamp_utc
    ) - my_rating as rating_change

  from {{ ref("int_game_information") }}
),

fct_game as (
  select
    game_sid,
    opening_sid,
    time_control_sid
      as opponent_sid,
    result_sid,
    my_rating,
    opponent_rating,
    my_accuracy,
    opponent_accuracy,
    move_number_reached,
    total_moves,
    game_length,
    rating_change

  from fact_columns

  inner join joining_dims
    on fact_columns.game_id = joining_dims.game_id
)

select * from fct_game
