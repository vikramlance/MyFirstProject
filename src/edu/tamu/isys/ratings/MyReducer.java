package edu.tamu.isys.ratings;

/*
 * This is reducer class to convert the input from Mapper into key value pairs 
 * output format for reducer is as follows 
 * key =  movie genre , value = highest rated "movie name" and its "rating" corresponding to the genre
 * 
 */

import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer < Text, Text, Text, Text > {


	public void reduce(Text key, Iterable < Text > values, Context context) throws IOException, InterruptedException {

		List < String > movieNames = new ArrayList < String > (); //collect all the distinct movie names in an list

		List < String > inputList = new ArrayList < String > (); //collect the input string in the list
		for (Text value: values) {
			String line = value.toString();
			String[] parts = line.split("::");

			String movie = parts[0];
			inputList.add(value.toString());

			if (!movieNames.contains(movie)) {
				movieNames.add(movie);
			}

		}

		Double higestAverage = 0d; //Average of highest rated movie in a genre
		String topRatedMovie = ""; //Name of highest rated movie in a genre

		int i = 0;
		while (i < movieNames.size()) {

			Double sum = 0d;
			int counter = 0;
			String movieNew = ""; // movie name 
			Integer ratingNew = 0; // movie rating

			for (int j = 0; j < inputList.size(); j++)

			{

				String[] partsNew = inputList.get(j).split("::");
				movieNew = partsNew[0];
				ratingNew = Integer.parseInt(partsNew[1]);
				if (movieNew.equals(movieNames.get(i))) { 
					sum += ratingNew;
					counter += 1;
					
				}
			}


			System.out.println(counter);

			// Calculate average of each movie
			Double average = sum / counter; 
			
			//If average of greater than previous highest average then replace it with new average
			if (higestAverage < average) {
				higestAverage = average;
				topRatedMovie = movieNames.get(i);


			}

			i++;

		}
		// Print out put as genre name , top rated movie and its average rating in that genre
		context.write(key, new Text(topRatedMovie + " " + "(" + higestAverage + ")"));
	}

}