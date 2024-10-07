with

raw_results as (
  select
    game_id,
    game_winner,
    game_status,
    colour,
    case
      when colour = game_winner then 'w'
      when game_winner = 'draw' then 'd'
      else 'l'
    end as result

  from {{ source ("lichess", "raw_games_lichess") }}
),

mapped_results as (
  select
    raw_results.game_id,
    result_mapping.my_result,
    result_mapping.opponent_result,
    result_mapping.result_method

  from raw_results

  left join {{ ref("lichess__result_mapping") }} as result_mapping
    on raw_results.result = result_mapping.result
    and raw_results.game_status = result_mapping.game_status
)

select * from mapped_results
