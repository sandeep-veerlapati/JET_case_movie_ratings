-- What are the top 5 and bottom 5 movies in terms of overall average review ratings for a given month?


WITH avg_ratings AS
  (SELECT asin,
          YEAR,
          MONTH,
          title,
          avg(ratings) AS avg_rating
   FROM op
   WHERE YEAR=2003
     AND MONTH=5
   GROUP BY asin,YEAR,MONTH,title)

SELECT asin, title, avg_rating from (
SELECT *,
       row_number() OVER (ORDER BY avg_rating DESC, title ASC) AS top_five,
       row_number() OVER (ORDER BY avg_rating ASC, title ASC) AS bottom_five
FROM avg_ratings where title is not null)
where (top_five <=5 or bottom_five <=5)
