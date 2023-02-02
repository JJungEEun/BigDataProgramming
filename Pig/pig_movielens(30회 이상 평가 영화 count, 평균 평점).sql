with rws as 
	(select title, cnt, rating_sum, genres str from 
	(select m.title, m.genres, r.cnt, r.rating_sum FROM movies m JOIN(
	 SELECT movieId, COUNT(*) as cnt, SUM(rating) as rating_sum FROM ratings GROUP BY movieId HAVING COUNT(*) >30) r ON m.movieId = r.movieId)),
	rws2 as
	 (select title, regexp_substr (str, '[^|]+', 1, level) genres, cnt, rating_sum from rws
	   	connect by level <= length (str) -  length ( replace ( str, '|' ) ) + 1)
 select sum(cnt) as cnt , sum(rating_sum)/sum(cnt) as rating from rws2 group by genres ORDER BY cnt DESC;
