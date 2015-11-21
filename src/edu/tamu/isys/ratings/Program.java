package edu.tamu.isys.ratings;


/*This program will take input file containing movie rating given by viewers and genre of the movie 
 * and returns top rated movies and their average rating in each genre.

 * @author Vikramsinh Jadhav
 * 
 * Input file format
 *  movie id :: movie name :: movie genre [,movie genre] :: user id :: user age :: user gender 
 *  :: rating :: timestamp
 *  
 * Output file format
 * <movie genre> < highest rated movie name in the genre > <Average rating og the movie>
 * 
 * Example
 * Action Days of Thunder (1990) (4.87) 
 * 
 * 

*/

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Program extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		// Initiation of program
		System.out.println("Start Processing the input file");
		int exitCode = ToolRunner.run(new Program(), args);
		System.out.println("Program ended with exit code: " + exitCode); //Print exit code
		System.exit(exitCode);
	}

	public int run(String args[]) throws Exception {
		Job conf = Job.getInstance(getConf(), "TopRatedMovies");
		conf.setJarByClass(Program.class);
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);


		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		return conf.waitForCompletion(true) ? 0 : 1;
	}


}