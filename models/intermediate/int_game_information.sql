with chesscom_game_info as (
  {{ get_intermediate_model("chesscom") }}
),

lichess_game_info as (
  {{ get_intermediate_model("lichess") }}
)

select * from chesscom_game_info
where
  result_method is not null
  and total_moves >= 2
union all
select * from lichess_game_info
where
  result_method is not null
  and total_moves >= 2
