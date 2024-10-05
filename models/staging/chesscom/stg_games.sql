{% set my_opponent_columns = ["accuracy", "rating"] %}
{% set my_or_opponent = ["my", "opponent"] %}

with games as (

  select
    game_id,
    url,
    is_rated,
    {# Create my_accuracy, opponent_accuracy, my_rating, opponent_rating #}
    {% for column in my_opponent_columns %}
      {% for type in my_or_opponent %}
        case
          when colour = 'white'
          {% if type == "my" %}
            then white_{{ column }}
          else black_{{ column }}
          {% else %}
            then black_{{ column }}
          else white_{{ column }}
          {% endif %}
        end as {{ type }}_{{ column }},
      {% endfor %}    
    {% endfor %}
    colour,
    opponent_id,
    opponent_username,
    opponent_country,
    opponent_is_verified,
    opponent_status,
    total_moves,
    moves

  from {{ source("chesscom", "raw_games_chesscom") }}

)

select * from games
