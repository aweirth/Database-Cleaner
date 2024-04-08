/*

CLEAN.SQL FILE FOR SCRAPERS

Note: As of the current version, the scraper dumping tables mlbpre, and game_results are not being erased after piping clean data over to new
tables. This is to ensure everything runs correctly first. ON CONFLICT DO NOTHING is being used to ensure information is not duplicated.

*/

-- Cleaning game_results raw_json and inserting into mlb_games
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
    (game_data -> 'game' ->> 'gamePk')::int AS game_id,
    (game_data -> 'game' ->> 'officialDate')::date AS official_date,
    (game_data -> 'game' ->> 'gameDate')::timestamptz AS game_time_stp,
    (game_data -> 'game' -> 'teams' -> 'away' -> 'team' ->> 'id')::int AS away_id,
    (game_data -> 'game' -> 'teams' -> 'away' ->> 'score')::int AS away_score,
    (game_data -> 'game' -> 'teams' -> 'home' -> 'team' ->> 'id')::int AS home_id,
    (game_data -> 'game' -> 'teams' -> 'home' ->> 'score')::int AS home_score,
    game_data -> 'game' -> 'status' ->> 'detailedState' AS d_state,
    game_data -> 'game' ->> 'dayNight' AS day_night,
    game_data -> 'game' ->> 'gameType' AS game_type,
    (game_data -> 'game' ->> 'gameNumber')::int AS game_number,
    game_data -> 'game' ->> 'description' AS description,
    (game_data -> 'game' -> 'venue' ->> 'id')::int AS venue_id
FROM tran
WHERE game_data -> 'game' -> 'status' ->> 'detailedState' = 'Final'
ON CONFLICT (game_id)
DO NOTHING;

-- Cleaning mlbpre table and inserting clean data into game_odds table
INSERT INTO game_odds
WITH json_rows AS (
    SELECT 
        raw_json -> 'id' AS game_id,
        (raw_json ->> 'commence_time')::timestamp with time zone AS commence_time,
        raw_json -> 'away_team' AS away_team,
        raw_json -> 'home_team' AS home_team,
        raw_json -> 'sport_key' AS sport_key,
        jsonb_array_elements(raw_json -> 'bookmakers') AS bookmaker
    FROM mlbpre
)
SELECT 
    game_id,
    commence_time,
    away_team,
    home_team,
    sport_key,
    (bookmaker ->> 'title')::text AS bookmaker,
    (bookmaker -> 'markets' -> 0 -> 'outcomes' -> 0 ->> 'name')::text AS team_name_1,
    (bookmaker -> 'markets' -> 0 -> 'outcomes' -> 0 ->> 'price')::numeric AS team_1_price,
    (bookmaker -> 'markets' -> 0 -> 'outcomes' -> 1 ->> 'name')::text AS team_name_2,
    (bookmaker -> 'markets' -> 0 -> 'outcomes' -> 1 ->> 'price')::numeric AS team_2_price
FROM json_rows
ON CONFLICT (game_id, bookmaker)
DO NOTHING
;
