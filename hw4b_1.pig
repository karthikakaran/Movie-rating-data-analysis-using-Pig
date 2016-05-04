inpUsers = LOAD 'hdfs://cshadoop1/Spring-2016-input/users.dat' using PigStorage(':') as (UserID:int,Gender:chararray,Age:int,Occupation:int,Zipcode:chararray);
inpMovies = LOAD 'hdfs://cshadoop1/Spring-2016-input/movies.dat' using PigStorage(':') as (MovieID:int,Title:chararray,Genres:chararray);
inpRatings = LOAD 'hdfs://cshadoop1/Spring-2016-input/ratings.dat' using PigStorage(':') as (UserID:int,MovieID:int,Rating:int);

MovieRatings = join inpMovies by(MovieID), inpRatings by(MovieID);
WarAction = filter MovieRatings by (Genres matches '.*War.*' and Genres matches '.*Action.*');

groupA = group WarAction by $0;
groupB = foreach groupA generate flatten(group), AVG(WarAction.$5);
groupdesc = order groupB by $1 asc;

Limitdesc = limit groupdesc 1;
A= foreach Limitdesc generate $1;

jmovie= join groupB by ($1), A by ($0);
jratings = join inpRatings by(MovieID), jmovie by($0);
juserratings = join jratings by($0), inpUsers by($0);

Age = filter juserratings by (Gender matches '.*F.*' and (Age > 20 AND Age < 35) and Zipcode matches '1.*');
final_userID = foreach Age generate $0;
final = distinct final_userID;
dump final;
