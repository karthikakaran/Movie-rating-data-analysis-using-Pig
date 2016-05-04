
inpMovie = LOAD 'hdfs://cshadoop1/Spring-2016-input/movies.dat' using PigStorage(':') as (MovieID:int,Title:chararray,Genres:chararray);
inpRating = LOAD 'hdfs://cshadoop1/Spring-2016-input/ratings.dat' using PigStorage(':') as (UserID:int,MovieID:int,Rating:int);

coGroupByMovieID = COGROUP inpMovie BY MovieID, inpRating BY MovieID;
firstFiveIds = LIMIT coGroupByMovieID 5;
dump firstFiveIds;
