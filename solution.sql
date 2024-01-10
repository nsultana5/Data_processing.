create table users(
	userid int primary key,
	name text not null
);


create table movies(

movieid int primary key,
title text not null
);

create table taginfo(

tagid int primary key,
content text not null
);

create table genres(

genreid int primary key,
name text not null
);


create table ratings(

userid int  references users(userid),
movieid int references movies(movieid),
rating numeric check(rating>=0 and rating<=5),
timestamp bigint,
primary key(userid,movieid)

);

create table tags(

userid int references users(userid),
movieid int references movies(movieid),
tagid int references taginfo(tagid),
timestamp bigint,
primary key(userid,movieid,tagid)
);

create table hasagenre(

movieid int references movies(movieid),
genreid int references genres(genreid),
primary key(movieid,genreid)
);


