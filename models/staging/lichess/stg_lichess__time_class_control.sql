with time_classes as (

  select
    game_id,
    time_class,
    time_control,
    -- 3+0, 5+1 etc
    split(time_control, '+')[safe_offset(0)] as main,
    split(time_control, '+')[safe_offset(1)] as increment

  from {{ source("lichess", "raw_games_lichess") }}

  where time_class in ('ultraBullet', 'bullet', 'rapid', 'blitz', 'classical')

),

minutes_and_seconds as (

  select
    game_id,
    time_class,
    main                                  as seconds,
    cast(coalesce(increment, '0') as int) as increment,
    cast(main as int) / 60                as minutes

  from time_classes

),

all_data as (

  select
    game_id,
    cast(minutes as int)           as minutes,
    cast(seconds as int)           as seconds,
    increment                      as increment_amount,
    initcap(time_class)            as time_class,
    concat(
      minutes,
      '+',
      increment
    )                              as time_control,
    coalesce(increment > 0, false) as is_increment

  from minutes_and_seconds

)

select * from all_data
