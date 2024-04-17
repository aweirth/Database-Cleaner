/*

CLEAN.SQL FILE FOR SCRAPERS

Note: As of the current version, the scraper dumping tables mlbpre, and game_results are not being erased after piping clean data over to new
tables. This is to ensure everything runs correctly first. ON CONFLICT DO NOTHING is being used to ensure information is not duplicated.

*/


-- PRICES TABLE

WITH json_rows AS (
    SELECT 
        (raw_json ->> 'id')::TEXT AS game_id,
  			(raw_json ->> 'commence_time')::timestamp with time zone AS commence_time,
        jsonb_array_elements(raw_json -> 'bookmakers') AS bookmaker
    FROM mlbpre
)
INSERT INTO prices
SELECT 
    game_id,
    commence_time,
    (bookmaker ->> 'title')::text AS bookmaker,
    (bookmaker -> 'markets' -> 0 -> 'outcomes' -> 0 ->> 'name')::text AS team_name_1,
    (bookmaker -> 'markets' -> 0 -> 'outcomes' -> 0 ->> 'price')::numeric AS team_1_price,
    (bookmaker -> 'markets' -> 0 -> 'outcomes' -> 1 ->> 'name')::text AS team_name_2,
    (bookmaker -> 'markets' -> 0 -> 'outcomes' -> 1 ->> 'price')::numeric AS team_2_price
FROM json_rows
ON CONFLICT (game_id)
DO NOTHING
;

-- GAME KEY TABLE

INSERT INTO game_keys
SELECT 
		(raw_json ->> 'id')::TEXT AS game_id,
    (raw_json ->> 'commence_time')::timestamp with time zone AS commence_time,
    (raw_json ->> 'away_team')::TEXT AS away_team,        
    (raw_json ->> 'home_team')::TEXT AS home_team
FROM mlbpre
ON CONFLICT (game_id)
DO NOTHING
;

-- MLB GAMES TABLE

WITH json_rows AS (
    SELECT jsonb_array_elements(raw_json -> 'dates' -> 0 -> 'games') AS game
    FROM game_results
),
tran AS (
    SELECT json_build_object('game', game) AS game_data
    FROM json_rows
)
INSERT INTO mlb_games
SELECT
		gk.game_id,
    (game_data -> 'game' ->> 'officialDate')::date AS official_date,
    (game_data -> 'game' ->> 'gameDate')::timestamptz AT TIME ZONE 'UTC' AS game_time_stp,
    (game_data -> 'game' -> 'teams' -> 'away' -> 'team' ->> 'name') AS away_name,
    (game_data -> 'game' -> 'teams' -> 'away' ->> 'score')::int AS away_score,
    (game_data -> 'game' -> 'teams' -> 'home' -> 'team' ->> 'name') AS home_name,
    (game_data -> 'game' -> 'teams' -> 'home' ->> 'score')::int AS home_score,
    game_data -> 'game' -> 'status' ->> 'detailedState' AS game_state,
    game_data -> 'game' ->> 'dayNight' AS day_night,
    game_data -> 'game' ->> 'gameType' AS game_type,
    (game_data -> 'game' ->> 'gameNumber')::int AS game_number,
    game_data -> 'game' ->> 'description' AS description,
    (game_data -> 'game' -> 'venue' ->> 'name') AS venue_name
FROM 
    tran
INNER JOIN 
    game_keys AS gk ON (game_data -> 'game' -> 'teams' -> 'away' -> 'team' ->> 'name' = gk.away_team
                      AND game_data -> 'game' -> 'teams' -> 'home' -> 'team' ->> 'name' = gk.home_team
                      AND (game_data -> 'game' ->> 'gameDate')::timestamptz AT TIME ZONE 'UTC' = gk.commence_time)
WHERE 
    game_data -> 'game' -> 'status' ->> 'detailedState' = 'Final'
ON CONFLICT (game_id)
DO NOTHING
;
