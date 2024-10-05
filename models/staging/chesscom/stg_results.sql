with

results as (
  select
    games.game_id,
    result_mapping.my_result,
    result_mapping.opponent_result,
    result_mapping.result_method

  from {{ source("chesscom", "raw_games_chesscom") }} as games

  left join
    {{ ref("result_mapping_chesscom") }} as result_mapping
    on games.colour = result_mapping.colour
    and games.white_result = result_mapping.white_result
    and games.black_result = result_mapping.black_result
)

select * from results