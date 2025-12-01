
-- This document contains data cleaning and transformation queries used in BigQuery for exploratory analysis focused on Cyclistic electric scooters, with some comparisons to other vehicle types. Each query is commented for clarity and organized by purpose. While this analysis is not part of the main case study, it offers potential leads for future research.



-- Create a copy of the table to work with

CREATE TABLE `project-name.cyclistic.all_2024_trips_scooters` AS

SELECT
  *
FROM
  `project-name.cyclistic.all_2024_trips`
;



-- Remove the duplicates from the table


CREATE OR REPLACE TABLE `project-name.cyclistic.all_2024_trips_scooters` AS

WITH duplicates AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY started_at) AS row_num
  FROM `project-name.cyclistic.all_2024_trips_scooters`
)
SELECT
  *

FROM
  duplicates

WHERE
  row_num = 1
;


-- Create a column "ride_length"

CREATE OR REPLACE TABLE `project-name.cyclistic.all_2024_trips_scooters` AS

SELECT
  *,
  TIMESTAMP_DIFF(ended_at, started_at, MINUTE) AS ride_length

FROM
  `project-name.cyclistic.all_2024_trips_scooters`
;


-- Create a column "day_of_the_week" with 1 = Sunday & 7 = Saturday


CREATE OR REPLACE TABLE `project-name.cyclistic.all_2024_trips_scooters` AS

SELECT
  *,
  EXTRACT(DAYOFWEEK FROM started_at) as day_of_the_week

FROM
  `project-name.cyclistic.all_2024_trips_scooters`
;




-- Average ride length between casual and member riders


SELECT
  AVG(ride_length),
  member_casual

FROM
  `project-name.cyclistic.all_2024_trips_scooters`

WHERE
  ride_length != 0
  AND ride_length IS NOT NULL

GROUP BY
member_casual
;


-- Average ride length between casual and member riders per day of the week

WITH member_rides AS (
  SELECT
    AVG(ride_length) AS avg_member_ride_length,
    day_of_the_week
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
  member_casual = "member"
  AND ride_length > 0
  GROUP BY
  day_of_the_week
),

casual_rides AS (
  SELECT
    day_of_the_week,
    AVG(ride_length) AS avg_casual_ride_length
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
  member_casual = "casual"
  AND ride_length > 0
  GROUP BY
  day_of_the_week
)

SELECT
  avg_member_ride_length,
  avg_casual_ride_length,
  member_rides.day_of_the_week
FROM
  member_rides
FULL OUTER JOIN
  casual_rides ON member_rides.day_of_the_week = casual_rides.day_of_the_week

ORDER BY
  day_of_the_week
;


-- Average ride length between casual and member riders per month

WITH member_rides AS (
  SELECT
    EXTRACT(MONTH FROM started_at) AS month,
    ROUND(AVG(ride_length), 2) AS avg_ride_length
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
  member_casual = "member"
  AND ride_length > 0
  GROUP BY month
),

casual_rides AS (
  SELECT
    EXTRACT(MONTH FROM started_at) AS month,
    ROUND(AVG(ride_length), 2) AS avg_ride_length
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
  member_casual = "casual"
  AND ride_length > 0
  GROUP BY month
)


SELECT
  COALESCE(mr.month, cr.month) AS month,
  mr.avg_ride_length AS member_avg_ride_length,
  cr.avg_ride_length AS casual_avg_ride_length

FROM
  member_rides AS mr
FULL OUTER JOIN
  casual_rides AS cr ON mr.month = cr.month

ORDER BY
  month
;


-- Total rides between casual and member riders


WITH member_rides AS (
  SELECT
    COUNT(*) AS total_member_rides
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
  member_casual = "member"
),

casual_rides AS (
  SELECT
    COUNT(*) AS total_casual_rides
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
  member_casual = "casual"
)

SELECT
  total_member_rides,
  total_casual_rides
FROM
  member_rides, casual_rides
;


-- Total rides between casual and member riders per day of the week


WITH member_rides AS (
  SELECT
    day_of_the_week,
    COUNT(*) AS total_member_rides
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
    member_casual = "member"
  GROUP BY
    day_of_the_week
),

casual_rides AS (
  SELECT
    day_of_the_week,
    COUNT(*) AS total_casual_rides
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
    member_casual = "casual"
  GROUP BY
    day_of_the_week
)

SELECT
  COALESCE(mr.day_of_the_week, cr.day_of_the_week) AS day_of_the_week,
  mr.total_member_rides,
  cr.total_casual_rides

FROM
  member_rides AS mr
FULL OUTER JOIN
  casual_rides AS cr ON mr.day_of_the_week = cr.day_of_the_week

ORDER BY
  day_of_the_week
;


-- Total rides between casual and member riders per start station


WITH member_rides AS (
  SELECT
    start_station_name,
    COUNT(*) AS total_member_rides
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
    member_casual = "member"
    AND start_station_name IS NOT NULL
  GROUP BY start_station_name
),

casual_rides AS (
  SELECT
    start_station_name,
    COUNT(*) AS total_casual_rides
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
    member_casual = "casual"
    AND start_station_name IS NOT NULL
  GROUP BY start_station_name
)

SELECT
  COALESCE(mr.start_station_name, cr.start_station_name) AS start_station_name,
  mr.total_member_rides,
  cr.total_casual_rides

FROM
  member_rides mr
FULL OUTER JOIN
  casual_rides cr
ON
  mr.start_station_name = cr.start_station_name

ORDER BY
  (IFNULL(mr.total_member_rides, 0) + IFNULL(cr.total_casual_rides, 0)) DESC
;


-- Total rides between casual and member riders per end station


WITH member_rides AS (
  SELECT
    end_station_name,
    COUNT(*) AS total_member_rides
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
    member_casual = "member"
    AND end_station_name IS NOT NULL
  GROUP BY end_station_name
),

casual_rides AS (
  SELECT
    end_station_name,
    COUNT(*) AS total_casual_rides
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
    member_casual = "casual"
    AND end_station_name IS NOT NULL
  GROUP BY end_station_name
)

SELECT
  COALESCE(mr.end_station_name, cr.end_station_name) AS end_station_name,
  mr.total_member_rides,
  cr.total_casual_rides

FROM
  member_rides mr
FULL OUTER JOIN
  casual_rides cr
ON
  mr.end_station_name = cr.end_station_name

ORDER BY
  (IFNULL(mr.total_member_rides, 0) + IFNULL(cr.total_casual_rides, 0)) DESC
;



-- Total use of each vehicle between casual and member riders (number of rides)


WITH member_rides AS (
  SELECT
    rideable_type AS mr_rideable_type,
    COUNT(*) AS total_rides
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
  member_casual = "member"
  AND ride_length > 0
  GROUP BY
  rideable_type
),

casual_rides AS (
  SELECT
    rideable_type AS cr_rideable_type,
    COUNT(*) AS total_rides
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
  member_casual = "casual"
  AND ride_length > 0
  GROUP BY
  rideable_type
)

SELECT
  COALESCE(mr.mr_rideable_type, cr.cr_rideable_type) AS rideable_type,
  mr.total_rides AS member_rides,
  cr.total_rides AS casual_rides

FROM
  member_rides AS mr
FULL OUTER JOIN
  casual_rides AS cr ON mr.mr_rideable_type = cr.cr_rideable_type
;


-- Total rides between casual and member riders per hour for the 7 - 9 AM and 4 - 6 PM periods


WITH member_rides AS (
  SELECT
    EXTRACT(HOUR FROM started_at) AS member_hour,
    COUNT(*) AS total_member_rides
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
  member_casual = "member"
  GROUP BY
    member_hour
  HAVING
    member_hour BETWEEN 7 AND 9
    OR member_hour BETWEEN 16 AND 18
),

casual_rides AS (
  SELECT
    EXTRACT(HOUR FROM started_at) AS casual_hour,
    COUNT(*) AS total_casual_rides
  FROM
    `project-name.cyclistic.all_2024_trips_scooters`
  WHERE
  member_casual = "casual"
  GROUP BY
    casual_hour
  HAVING
    casual_hour BETWEEN 7 AND 9
    OR casual_hour BETWEEN 16 AND 18
)

SELECT
  COALESCE(mr.member_hour, cr.casual_hour) as hour,
  total_member_rides,
  total_casual_rides

FROM
  member_rides AS mr
FULL OUTER JOIN
  casual_rides AS cr ON mr.member_hour = cr.casual_hour
ORDER BY
  hour
;


-- Top start stations with the most casual riders


SELECT
  start_station_name,
  COUNT(*) AS total_casual_riders

FROM
  `project-name.cyclistic.all_2024_trips_scooters`

WHERE
  start_station_name IS NOT NULL
  AND member_casual = "casual"

GROUP BY
  start_station_name

ORDER BY 
  total_casual_riders DESC

LIMIT 20
;


-- Total rides between casual and member riders for each vehicle for 08/2024 & 09/2024 (including electric scooters)


WITH member_rides AS (
  SELECT
    rideable_type,
    EXTRACT(MONTH FROM started_at) AS month,
    COUNT(*) AS total_rides
  FROM
    `project-name.cyclistic.all_2024_trips_copy`
  WHERE
    member_casual = "member"
    AND ride_length > 0
  GROUP BY
    rideable_type, month
  HAVING
    month BETWEEN 8 AND 9
),

casual_rides AS (
  SELECT
    rideable_type,
    EXTRACT(MONTH FROM started_at) AS month,
    COUNT(*) AS total_rides
  FROM
    `project-name.cyclistic.all_2024_trips_copy`
  WHERE
    member_casual = "casual"
    AND ride_length > 0
  GROUP BY
    rideable_type, month
  HAVING
    month BETWEEN 8 AND 9
)

SELECT
  COALESCE(mr.MONTH, cr.MONTH) AS month,
  COALESCE(mr.rideable_type, cr.rideable_type) AS rideable_type,
  mr.total_rides AS member_rides,
  cr.total_rides AS casual_rides

FROM
  member_rides AS mr

FULL OUTER JOIN
  casual_rides AS cr
    ON mr.rideable_type = cr.rideable_type
    AND mr.MONTH = cr.MONTH

ORDER BY
  month, rideable_type
;


-- Total number of rides on electric scooters per month


SELECT
  rideable_type,
  EXTRACT(MONTH FROM started_at) AS month,
  COUNT(*) AS scooter_rides

FROM
  `project-name.cyclistic.all_2024_trips_copy`

WHERE
  rideable_type = "electric_scooter"

GROUP BY
  month,
  rideable_type
;


-- Total number of electric scooters rides between casual and member riders (08/2024 & 09/2024) per month


SELECT
  member_casual,
  EXTRACT(MONTH FROM started_at) AS month,
  COUNT(*) AS scooter_rides

FROM
  `project-name.cyclistic.all_2024_trips_copy`

WHERE
  rideable_type = "electric_scooter"

GROUP BY
  member_casual, month
;


-- Total number of electric scooter rides on each day of the week

SELECT
  day_of_the_week,
  COUNT(*) AS scooter_rides

FROM
  `project-name.cyclistic.all_2024_trips_copy`

WHERE
  rideable_type = "electric_scooter"

GROUP BY
  day_of_the_week

ORDER BY
  day_of_the_week
;


-- Total number of electric scooter rides between casual and member riders on each day of the week


WITH member_rides AS (
  SELECT
    rideable_type,
    day_of_the_week,
    COUNT(*) AS total_rides
  FROM
    `project-name.cyclistic.all_2024_trips_copy`
  WHERE
  member_casual = "member"
  AND rideable_type = "electric_scooter"
  AND ride_length > 0
  GROUP BY
  rideable_type, day_of_the_week
),

casual_rides AS (
  SELECT
    rideable_type,
    day_of_the_week,
    COUNT(*) AS total_rides
  FROM
    `project-name.cyclistic.all_2024_trips_copy`
  WHERE
  member_casual = "casual"
  AND rideable_type = "electric_scooter"
  AND ride_length > 0
  GROUP BY
  rideable_type, day_of_the_week
)

SELECT
  COALESCE(mr.day_of_the_week, cr.day_of_the_week) AS day_of_the_week,
  COALESCE(mr.rideable_type, cr.rideable_type) AS rideable_type,
  mr.total_rides AS member_rides,
  cr.total_rides AS casual_rides

FROM
  member_rides AS mr

FULL OUTER JOIN
  casual_rides AS cr
    ON mr.rideable_type = cr.rideable_type
    AND mr.day_of_the_week = cr.day_of_the_week

ORDER BY
  day_of_the_week, rideable_type
;



-- Total number of electric scooter rides between casual and member riders per hour on weekdays


WITH member_rides AS (
  SELECT
    EXTRACT(HOUR FROM started_at) AS member_hour,
    COUNT(*) AS total_member_rides
  FROM
    `project-name.cyclistic.all_2024_trips_copy`
  WHERE
  member_casual = "member"
  AND rideable_type = "electric_scooter"
  AND day_of_the_week BETWEEN 2 AND 6
  GROUP BY
    member_hour
),

casual_rides AS (
  SELECT
    EXTRACT(HOUR FROM started_at) AS casual_hour,
    COUNT(*) AS total_casual_rides
  FROM
    `project-name.cyclistic.all_2024_trips_copy`
  WHERE
  member_casual = "casual"
  AND rideable_type = "electric_scooter"
  AND day_of_the_week BETWEEN 2 AND 6
  GROUP BY
    casual_hour
)

SELECT
  COALESCE(mr.member_hour, cr.casual_hour) as hour,
  total_member_rides,
  total_casual_rides

FROM
  member_rides AS mr
FULL OUTER JOIN
  casual_rides AS cr ON mr.member_hour = cr.casual_hour
ORDER BY
  hour
;


-- Total number of electric scooter rides between casual and member riders per hour on weekend days


WITH member_rides AS (
  SELECT
    EXTRACT(HOUR FROM started_at) AS member_hour,
    COUNT(*) AS total_member_rides
  FROM
    `project-name.cyclistic.all_2024_trips_copy`
  WHERE
  member_casual = "member"
  AND rideable_type = "electric_scooter"
  AND day_of_the_week IN(7, 1)
  GROUP BY
    member_hour
),

casual_rides AS (
  SELECT
    EXTRACT(HOUR FROM started_at) AS casual_hour,
    COUNT(*) AS total_casual_rides
  FROM
    `project-name.cyclistic.all_2024_trips_copy`
  WHERE
  member_casual = "casual"
  AND rideable_type = "electric_scooter"
  AND day_of_the_week IN(7, 1)
  GROUP BY
    casual_hour
)

SELECT
  COALESCE(mr.member_hour, cr.casual_hour) as hour,
  total_member_rides,
  total_casual_rides

FROM
  member_rides AS mr
FULL OUTER JOIN
  casual_rides AS cr ON mr.member_hour = cr.casual_hour
ORDER BY
  hour
;



-- Average ride length between member and casual riders by vehicle


WITH member_rides AS (
  SELECT
    rideable_type,
    ROUND(AVG(ride_length), 2) AS member_ride_length,
    COUNT(*) AS total_rides
  FROM
    `project-name.cyclistic.all_2024_trips_copy`
  WHERE
  member_casual = "member"
  AND ride_length > 0
  GROUP BY
  rideable_type
),

casual_rides AS (
  SELECT
    rideable_type,
    ROUND(AVG(ride_length), 2) AS casual_ride_length,
    COUNT(*) AS total_rides
  FROM
    `project-name.cyclistic.all_2024_trips_copy`
  WHERE
  member_casual = "casual"
  AND ride_length > 0
  GROUP BY
  rideable_type
)

SELECT
  COALESCE(mr.rideable_type, cr.rideable_type) AS rideable_type,
  mr.member_ride_length AS avg_member_ride_length,
  mr.total_rides AS member_rides,
  cr.casual_ride_length AS avg_casual_ride_length,
  cr.total_rides AS casual_rides

FROM
  member_rides AS mr

FULL OUTER JOIN
  casual_rides AS cr
    ON mr.rideable_type = cr.rideable_type

ORDER BY
  rideable_type
;





-- Remove the unnecessary column(s)

ALTER TABLE
  `project-name.cyclistic.all_2024_trips_copy`

DROP COLUMN
  row_num
;
