package edu.tamu.isys.ratings;

/*
 * This is mapper class to convert the input from file into key value pairs 
 * output format for mapper is as follows 
 * key =  movie genre , value = list of all the "movie name :: rating" corresponding to genre
 * 
 */

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;


public class MyMapper extends Mapper < LongWritable, Text, Text, Text > {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 
		String line = value.toString();
		String[] parts = line.split("::"); //create an array of all elements delimited by "::"

		/* Check if given input line is having at least 8 elements as specified in readme.txt
				to handle run time exceptions */
		if (parts.length > 7) {
			String genre = parts[2]; //assign value of genre to variable 
			if (!genre.isEmpty()) {
				String[] genreparts = genre.split(",");


				String movieNameRating;
				for (int i = 0; i < genreparts.length; i++) {
					movieNameRating = parts[1] + "::" + parts[6];
					context.write(new Text(genreparts[i]), new Text(movieNameRating));

				}

			}
		}

	}
}