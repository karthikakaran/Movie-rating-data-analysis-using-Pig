//Q1:
inpUser = LOAD 'hdfs://cshadoop1/Spring-2016-input/users.dat' using PigStorage(':') as (UserID:int,Gender:chararray,Age:int,Occupation:int,ZipCode:chararray);
femaleAndAgeFilter = filter inpUser by Gender eq 'F' and Age >= 20 and Age <= 35 and ZipCode matches '1*';

inpMovie = LOAD 'hdfs://cshadoop1/Spring-2016-input/movies.dat' using PigStorage(':') as (MovieID:int,Title:chararray,Genres:chararray);
warAndAction = filter inpMovie by Genres matches '.*(War).*' and Genres matches '.*(Action).*';

inpRating = LOAD 'hdfs://cshadoop1/Spring-2016-input/ratings.dat' using PigStorage(':') as (UserID:int,MovieID:int,Rating:int);

comDramaIds = JOIN warAndAction BY MovieID, inpRating BY MovieID;
cdRatingAndDetails = FOREACH comDramaIds GENERATE inpRating.MovieID as MovieID, Genres as Genres, UserID as UserID, Rating as Rating;

groupMovie = GROUP cdRatingAndDetails BY MovieID;
//idAndMin = FOREACH groupMovie generate group as MovieId, MIN(cdRatingAndDetails.Rating) as minRating;
idAndAvg = FOREACH groupMovie generate flatten(group), AVG(cdRatingAndDetails.Rating) as avgRating;

groupasc = order idAndAvg by $1 asc;
Limitasc = limit groupasc 1;
A = foreach Limitasc generate $1;

minRatings = join idAndAvg by ($1), A by ($0);
//jratings = join cdRatingAndDetails by(MovieID), jmovie by($0);
lowestRatingUsers = join minRatings by($0), inpUser by($0);

//lowestRatingUsers = FILTER cdRatingAndDetails BY MovieID == idAndAvg.MovieId and Rating == idAndAvg.minRating;
finalUserIds = JOIN femaleAndAgeFilter BY UserID, lowestRatingUsers BY UserID;

finalUserIdsR = FOREACH finalUserIds GENERATE femaleAndAgeFilter.UserID as UserID;
distinctUsers = DISTINCT finalUserIdsR;
dump distinctUsers;


//Q2:
inpMovie = LOAD 'hdfs://cshadoop1/Spring-2016-input/movies.dat' using PigStorage(':') as (MovieID:int,Title:chararray,Genres:chararray);
inpRating = LOAD 'hdfs://cshadoop1/Spring-2016-input/ratings.dat' using PigStorage(':') as (UserID:int,MovieID:int,Rating:int);

coGroupByMovieID = COGROUP inpMovie BY MovieID, inpRating BY MovieID;
firstFiveIds = LIMIT coGroupByMovieID 5;
dump firstFiveIds;


//Q3:
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class FORMAT_GENRE extends EvalFunc <String> {
    @Override
    public String exec(Tuple input) {
		String formattedStr = "";
        try {
            if (input == null || input.size() == 0) {
                return null;
            }
        String str = (String) input.get(0);
	String splitStr[] = str.split("\\|");
	    for(int i = 0; i < splitStr.length; i++){
	    	formattedStr += (i+1)+") "+splitStr[i];
	    	if(i != 0 && i == splitStr.length - 2){
	    		formattedStr += " & ";
	    	} else if(i == 0 && i == splitStr.length - 2){
	    		formattedStr += " & ";
	    	} else if(i != 0 && i < splitStr.length - 2){
	    		formattedStr += ", ";
	    	} else if (i == 0 && i != splitStr.length - 1){
	    		formattedStr += ", ";
	    	} 
	    } 
        return formattedStr;
        } catch (ExecException ex) {
            System.out.println("Error: " + ex.toString());
        }
        return null;
    }
}

{cs6360:~} mkdir PIG_UDF
{cs6360:~} cd PIG_UDF

Copy the FORMAT_GENRE.java file to this directory
{cs6360:~/PIG_UDF} javac -cp /usr/local/pig-0.13.0/pig-0.13.0-h1.jar FORMAT_GENRE.java
Create the jar file
{cs6360:~/PIG_UDF} jar -cf pig_udf.jar FORMAT_GENRE.class

REGISTER  /home/010/k/kx//kxk152430/PIG_UDF/pig_udf.jar;
inpMovie = LOAD 'hdfs://cshadoop1/Spring-2016-input/movies.dat' using PigStorage(':') as (MovieID:int,Title:chararray,Genres:chararray);
formatResult = FOREACH inpMovie GENERATE Title, FORMAT_GENRE(Genres);
DUMP formatResult;

