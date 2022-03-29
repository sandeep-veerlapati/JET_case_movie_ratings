--For a given month, what are the 5 movies whose average monthly ratings increased the most compared with the previous month?

WITH avg_ratings_current AS
  (SELECT asin,
          YEAR,
          MONTH,
          title,
          avg(ratings) AS avg_rating
   FROM op
   WHERE YEAR=2014
     AND MONTH=6
   GROUP BY 1,2,3,4),

avg_ratings_previous AS
  (SELECT asin,
          YEAR,
          MONTH,
          title,
          avg(ratings) AS avg_rating
   FROM op
   WHERE YEAR=2014
     AND MONTH=5
   GROUP BY 1,2,3,4),

final_ratings AS
  (SELECT ap.asin,
          ap.title,
          ac.avg_rating AS current_avg_rating,
          ap.avg_rating AS previous_avg_rating,
          (ac.avg_rating - ap.avg_rating) AS increase_in_rating
   FROM avg_ratings_current ac
   INNER JOIN avg_ratings_previous ap ON ac.asin = ap.asin)

SELECT asin, title,
       previous_avg_rating,
       current_avg_rating,
       increase_in_rating
FROM final_ratings
WHERE current_avg_rating > previous_avg_rating
and title is not null
ORDER BY increase_in_rating DESC
LIMIT 5