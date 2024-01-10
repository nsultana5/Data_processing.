create table query1 as
select g.name ,count(h.genreid)as moviecount
from genres g,hasagenre h
where g.genreid=h.genreid 
group by g.genreid;


create table query2 as
select g.name,avg(r.rating)as rating from genres g,ratings r 
where exists
(select h.genreid,h.movieid from hasagenre h where 
h.movieid=r.movieid
and h.genreid=g.genreid)
group by g.name;

create table temp1 as
select m.title,count(r.rating) as CountOfRatings from movies m,
(select movieid,rating from ratings)as r
where r.movieid=m.movieid group by m.title;
create table query3 as
select title,CountOfRatings from temp1 
where CountOfRatings>=10;

create table query4 as
select m.movieid,m.title 
from movies m 
where m.movieid in
(select h.movieid from hasagenre h  where h.genreid in
(select g.genreid from genres g 
where g.name='Comedy'))
group by m.movieid;

create table query5 as
select m.title,avg(r.rating) as average 
from movies m,
(select movieid ,rating from ratings)
as r where r.movieid=m.movieid
group by m.title;

create table query6 as
select avg(r.rating)as average
from ratings r 
where r.movieid in
(select h.movieid from hasagenre h where h.genreid in
(select g.genreid from genres g where g.name ='Comedy' ));

create table query7 as
select avg(r.rating)as average
from ratings r where r.movieid in 
((select h.movieid from hasagenre h where h.genreid in 
 (select g.genreid from genres g where g.name='Comedy')) intersect
(select h.movieid from hasagenre h where h.genreid in 
(select g.genreid from genres g where g.name='Romance')));

create table query8 as
select avg(r.rating)as average
from ratings r where 
r.movieid in
(select h.movieid from hasagenre h where h.genreid in
(select g.genreid from genres g where g.name='Romance'))
except(select h.movieid from hasagenre h where h.genreid in
(select g.genreid from genres g where g.name ='Comedy'));

create table query9 as
select movieid,rating 
from ratings 
where ratings.userid=:v1;

CREATE TABLE similarity as (
with avgcalc as (select movieid, avg(rating) as average
	from ratings
	group by movieid)
	select a.movieid as m1, b.movieid as m2, ( 1 - ( ABS ( a.average - b.average))/5) as s, c.rating, d.title
		from avgcalc as a, avgcalc as b ,query9 as c,movies as d
		where a.movieid not in(select movieid from query9) --select all movies not rated
		and b.movieid in (select movieid from query9) --select all movies rated
		and b.movieid = c.movieid 
		and a.movieid = d.movieid);

CREATE TABLE recommendation as (
select title
	from similarity 
	group by title, m1
	having (SUM(s * rating) /SUM(s)) > 3.9);





