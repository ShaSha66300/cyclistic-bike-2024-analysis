
-- This document contains all data cleaning and transformation queries used in BigQuery for the Cyclistic case study analysis. Each query is commented for clarity and grouped by purpose.



-- Assemble the data from each month in 1 dataset

CREATE OR REPLACE TABLE `project-name.cyclistic.all_2024_trips` AS

SELECT * FROM `project-name.cyclistic.202401_trips`

UNION ALL

SELECT * FROM `project-name.cyclistic.feb_trips`

UNION ALL

SELECT * FROM `project-name.cyclistic.mar_trips`

UNION ALL

SELECT * FROM `project-name.cyclistic.apr_trips`

UNION ALL

SELECT * FROM `project-name.cyclistic.may_trips`

UNION ALL

SELECT * FROM `project-name.cyclistic.jun_trips`

UNION ALL

SELECT * FROM `project-name.cyclistic.jul_trips`

UNION ALL

SELECT * FROM `project-name.cyclistic.aug_trips`

UNION ALL

SELECT * FROM `project-name.cyclistic.sep_trips`

UNION ALL

SELECT * FROM `project-name.cyclistic.oct_trips`

UNION ALL

SELECT * FROM `project-name.cyclistic.nov_trips`

UNION ALL

SELECT * FROM `project-name.cyclistic.dec_trips`
;



-- Create a copy of the table to work with

CREATE TABLE `project-name.cyclistic.all_2024_trips_no_scooters` AS

SELECT
  *
FROM
  `project-name.cyclistic.all_2024_trips`

WHERE
  rideable_type != "electric_scooter"
;



-- Remove the duplicates from the table


CREATE OR REPLACE TABLE `project-name.cyclistic.all_2024_trips_no_scooters` AS

WITH duplicates AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY started_at) AS row_num
  FROM `project-name.cyclistic.all_2024_trips_no_scooters`
)
SELECT
  *

FROM
  duplicates

WHERE
  row_num = 1
;



-- Create a column "ride_length"

CREATE OR REPLACE TABLE `project-name.cyclistic.all_2024_trips_no_scooters` AS

SELECT
  *,
  TIMESTAMP_DIFF(ended_at, started_at, MINUTE) AS ride_length

FROM
  `project-name.cyclistic.all_2024_trips_no_scooters`
;


-- Create a column "day_of_the_week" with 1 = Sunday & 7 = Saturday


CREATE OR REPLACE TABLE `project-name.cyclistic.all_2024_trips_no_scooters` AS

SELECT
  *,
  EXTRACT(DAYOFWEEK FROM started_at) as day_of_the_week

FROM
  `project-name.cyclistic.all_2024_trips_no_scooters`
;


-- Total number of rides

SELECT
  COUNT(*) AS total_rides

FROM
  `project-name.cyclistic.all_2024_trips_no_scooters`
;



-- Average ride length between casual and member riders (in minutes)


SELECT
  AVG(ride_length) AS avg_ride_length,
  member_casual

FROM
  `project-name.cyclistic.all_2024_trips_no_scooters`

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
    `project-name.cyclistic.all_2024_trips_no_scooters`
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
    `project-name.cyclistic.all_2024_trips_no_scooters`
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
    `project-name.cyclistic.all_2024_trips_no_scooters`
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
    `project-name.cyclistic.all_2024_trips_no_scooters`
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




-- Average ride length between casual and member riders per month for electric bikes


WITH member_rides AS (
  SELECT
    EXTRACT(MONTH FROM started_at) AS month,
    ROUND(AVG(ride_length), 2) AS avg_ride_length
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "member"
  AND rideable_type = "electric_bike"
  AND ride_length > 0
  GROUP BY month
),

casual_rides AS (
  SELECT
    EXTRACT(MONTH FROM started_at) AS month,
    ROUND(AVG(ride_length), 2) AS avg_ride_length
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "casual"
  AND rideable_type = "electric_bike"
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



-- Average ride length between casual and member riders per month for classic bikes


WITH member_rides AS (
  SELECT
    EXTRACT(MONTH FROM started_at) AS month,
    ROUND(AVG(ride_length), 2) AS avg_ride_length
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "member"
  AND rideable_type = "classic_bike"
  AND ride_length > 0
  GROUP BY month
),

casual_rides AS (
  SELECT
    EXTRACT(MONTH FROM started_at) AS month,
    ROUND(AVG(ride_length), 2) AS avg_ride_length
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "casual"
  AND rideable_type = "classic_bike"
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
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "member"
),

casual_rides AS (
  SELECT
    COUNT(*) AS total_casual_rides
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
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
    `project-name.cyclistic.all_2024_trips_no_scooters`
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
    `project-name.cyclistic.all_2024_trips_no_scooters`
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




-- Total rides between casual and member riders per month


WITH member_rides AS (
  SELECT
    EXTRACT(MONTH FROM started_at) as month,
    COUNT(*) AS total_member_rides
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
    member_casual = "member"
  GROUP BY
    month
),

casual_rides AS (
  SELECT
    EXTRACT(MONTH FROM started_at) as month,
    COUNT(*) AS total_casual_rides
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
    member_casual = "casual"
  GROUP BY
    month
)

SELECT
  COALESCE(mr.month, cr.month) AS month,
  mr.total_member_rides,
  cr.total_casual_rides

FROM
  member_rides AS mr
FULL OUTER JOIN
  casual_rides AS cr ON mr.month = cr.month

ORDER BY
  month
;






-- Total rides between casual and member riders per start station


WITH member_rides AS (
  SELECT
    start_station_name,
    COUNT(*) AS total_member_rides
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
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
    `project-name.cyclistic.all_2024_trips_no_scooters`
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
    `project-name.cyclistic.all_2024_trips_no_scooters`
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
    `project-name.cyclistic.all_2024_trips_no_scooters`
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
    `project-name.cyclistic.all_2024_trips_no_scooters`
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
    `project-name.cyclistic.all_2024_trips_no_scooters`
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


-- Total rides between casual and member riders per hour for the 7 - 9 AM and 4 - 6 PM periods for weekdays only


WITH member_rides AS (
  SELECT
    EXTRACT(HOUR FROM started_at) AS member_hour,
    COUNT(*) AS total_member_rides
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "member"
  AND day_of_the_week NOT IN(7, 1)
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
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "casual"
  AND day_of_the_week NOT IN(7, 1)
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



-- Total rides between casual and member riders per hour outside of the 7 - 9 AM and 4 - 6 PM periods for weekdays only


WITH member_rides AS (
  SELECT
    EXTRACT(HOUR FROM started_at) AS member_hour,
    COUNT(*) AS total_member_rides
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "member"
  AND day_of_the_week NOT IN(7, 1)
  GROUP BY
    member_hour
  HAVING
    member_hour NOT IN(7, 8, 9, 16, 17, 18)
),

casual_rides AS (
  SELECT
    EXTRACT(HOUR FROM started_at) AS casual_hour,
    COUNT(*) AS total_casual_rides
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "casual"
  AND day_of_the_week NOT IN(7, 1)
  GROUP BY
    casual_hour
  HAVING
    casual_hour NOT IN(7, 8, 9, 16, 17, 18)
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



-- Total rides between casual and member riders per hour for weekdays only


WITH member_rides AS (
  SELECT
    EXTRACT(HOUR FROM started_at) AS member_hour,
    COUNT(*) AS total_member_rides
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "member"
  AND day_of_the_week NOT IN(7, 1)
  GROUP BY
    member_hour
),

casual_rides AS (
  SELECT
    EXTRACT(HOUR FROM started_at) AS casual_hour,
    COUNT(*) AS total_casual_rides
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "casual"
  AND day_of_the_week NOT IN(7, 1)
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



-- Top start stations with the most casual riders


SELECT
  start_station_name,
  COUNT(*) AS total_casual_riders

FROM
  `project-name.cyclistic.all_2024_trips_no_scooters`

WHERE
  start_station_name IS NOT NULL
  AND member_casual = "casual"

GROUP BY
  start_station_name

ORDER BY 
  total_casual_riders DESC

LIMIT 20
;


-- Total rides between casual and member riders for each vehicle per month


WITH member_rides AS (
  SELECT
    rideable_type,
    EXTRACT(MONTH FROM started_at) AS month,
    COUNT(*) AS total_rides
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "member"
  AND ride_length > 0
  GROUP BY
  rideable_type, month
),

casual_rides AS (
  SELECT
    rideable_type,
    EXTRACT(MONTH FROM started_at) AS month,
    COUNT(*) AS total_rides
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "casual"
  AND ride_length > 0
  GROUP BY
  rideable_type, month
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



-- Total rides between casual and member riders for each vehicle by day of the week


WITH member_rides AS (
  SELECT
    rideable_type,
    day_of_the_week,
    COUNT(*) AS total_rides
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "member"
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
    `project-name.cyclistic.all_2024_trips_no_scooters`
  WHERE
  member_casual = "casual"
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




-- Average ride length between member and casual riders by vehicle


WITH member_rides AS (
  SELECT
    rideable_type,
    ROUND(AVG(ride_length), 2) AS member_ride_length,
    COUNT(*) AS total_rides
  FROM
    `project-name.cyclistic.all_2024_trips_no_scooters`
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
    `project-name.cyclistic.all_2024_trips_no_scooters`
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
  `project-name.cyclistic.all_2024_trips_no_scooters`

DROP COLUMN
  row_num
;
