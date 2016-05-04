REGISTER  /home/010/k/kx/kxk152430/PIG_UDF/pig_udf.jar;
inpMovie = LOAD 'hdfs://cshadoop1/Spring-2016-input/movies.dat' using PigStorage(':') as (MovieID:int,Title:chararray,Genres:chararray);
formatResult = FOREACH inpMovie GENERATE Title, FORMAT_GENRE(Genres);
DUMP formatResult;
