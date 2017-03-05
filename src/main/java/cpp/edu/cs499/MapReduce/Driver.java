package cpp.edu.cs499.MapReduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Driver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf(
					"Usage: %s needs two arguments, input and output files\n",
					getClass().getSimpleName());
			return -1;
		}

		// Find average ratings for each movie ID
		Job averageRatingJob = new Job();
		averageRatingJob.setJarByClass(Driver.class);
		averageRatingJob.setJobName("Total Ratings");

		FileInputFormat.addInputPath(averageRatingJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(averageRatingJob, new Path(args[1]
				+ "/TotalMovieRatings"));

		averageRatingJob.setOutputKeyClass(Text.class);
		averageRatingJob.setOutputValueClass(FloatWritable.class);
		averageRatingJob.setOutputFormatClass(TextOutputFormat.class);

		averageRatingJob.setMapperClass(TotalMovieRatingsMapClass.class);
		averageRatingJob.setReducerClass(AverageRatingReduceClass.class);

		int returnValue = averageRatingJob.waitForCompletion(true) ? 0 : 1;

		if (averageRatingJob.isSuccessful()) {
			System.out.println("Total Rating Job was successful");
		} else if (!averageRatingJob.isSuccessful()) {
			System.out.println("Total Rating Job was not successful");
		}

		// Order the top rated movies from best to worst
		Job topRatedMoviesJob = new Job();
		topRatedMoviesJob.setJarByClass(Driver.class);
		topRatedMoviesJob.setJobName("Top Rated Movies");

		FileInputFormat.addInputPath(topRatedMoviesJob, new Path(args[1]
				+ "/TotalMovieRatings" + "/part-r-00000"));
		FileOutputFormat.setOutputPath(topRatedMoviesJob, new Path(args[1]
				+ "/HighestRatedMovies"));

		topRatedMoviesJob.setSortComparatorClass(FloatWritableComparator.class);
		topRatedMoviesJob.setOutputKeyClass(FloatWritable.class);
		topRatedMoviesJob.setOutputValueClass(Text.class);
		topRatedMoviesJob.setOutputFormatClass(TextOutputFormat.class);

		topRatedMoviesJob.setMapperClass(TopRatedMoviesMapClass.class);

		returnValue = topRatedMoviesJob.waitForCompletion(true) ? 0 : 1;

		if (topRatedMoviesJob.isSuccessful()) {
			System.out.println("Top Rated Movies Job was successful");
		} else if (!topRatedMoviesJob.isSuccessful()) {
			System.out.println("Top Rated Movies Job was not successful");
		}

		// List top ten movies into text file
		getTopTenMovies(args[1]);

		// Count up total ratings by each user
		Job userRatingsJob = new Job();
		userRatingsJob.setJarByClass(Driver.class);
		userRatingsJob.setJobName("Total User Ratings Job");

		FileInputFormat.addInputPath(userRatingsJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(userRatingsJob, new Path(args[1]
				+ "/TotalUserRatings"));

		userRatingsJob.setOutputKeyClass(Text.class);
		userRatingsJob.setOutputValueClass(IntWritable.class);
		userRatingsJob.setOutputFormatClass(TextOutputFormat.class);

		userRatingsJob.setMapperClass(UserRatingsMapClass.class);
		userRatingsJob.setReducerClass(UserRatingsReduceClass.class);

		returnValue = userRatingsJob.waitForCompletion(true) ? 0 : 1;

		if (userRatingsJob.isSuccessful()) {
			System.out.println("Total User Ratings Job was successful");
		} else if (!userRatingsJob.isSuccessful()) {
			System.out.println("Total User Ratings Job was not successful");
		}

		// Order most ratings of users in descending order
		Job sortUserRatingsJob = new Job();
		sortUserRatingsJob.setJarByClass(Driver.class);
		sortUserRatingsJob.setJobName("Sort User Ratings Job");

		FileInputFormat.addInputPath(sortUserRatingsJob, new Path(args[1]
				+ "/TotalUserRatings" + "/part-r-00000"));
		FileOutputFormat.setOutputPath(sortUserRatingsJob, new Path(args[1]
				+ "/HighestUserRatings"));

		sortUserRatingsJob.setSortComparatorClass(IntWritableComparator.class);
		sortUserRatingsJob.setOutputKeyClass(IntWritable.class);
		sortUserRatingsJob.setOutputValueClass(Text.class);
		sortUserRatingsJob.setOutputFormatClass(TextOutputFormat.class);

		sortUserRatingsJob.setMapperClass(SortUserRatingsMapClass.class);

		returnValue = sortUserRatingsJob.waitForCompletion(true) ? 0 : 1;

		if (sortUserRatingsJob.isSuccessful()) {
			System.out.println("Sort User Ratings Job was successful");
		} else if (!sortUserRatingsJob.isSuccessful()) {
			System.out.println("Sort User Ratings Job was not successful");
		}
		
		
		//Prints Top 10 Users into TopTenUsers.txt
		getTopTenUsers(args[1]);

		return returnValue;
	}

	public static void getTopTenMovies(String args1) throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(args1
				+ "/HighestRatedMovies" + "/part-r-00000"));

		for (int i = 0; i < 10; i++) {
			String line = br.readLine();
			String[] split = line.split("\\s+");
			int movieID = Integer.parseInt(split[1]);
			float rating = Float.parseFloat(split[0]);

			try (Stream<String> lines = Files.lines(
					Paths.get("movie_titles.txt"), Charset.forName("Cp1252"))) {
				String movieLine = lines.skip(movieID - 1).findFirst().get();
				String[] movieLineSplit = movieLine.split(",");
				String movieName = movieLineSplit[2];

				try (FileWriter out = new FileWriter("TopTenMovies.txt", true)) {
					String first = i + 1 + ") " + movieName;
					String second = "Rating: " + rating;
					out.write(String.format("%-60s%s%n", first, second));
				}
			}
		}
		System.out.println("Top Ten Movies calculated in TopTenMovies.txt");
	}

	public static void getTopTenUsers(String args1) throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(args1
				+ "/HighestUserRatings" + "/part-r-00000"));

		for (int i = 0; i < 10; i++) {
			String line = br.readLine();
			String[] split = line.split("\\s+");
			String userID = split[1];
			int totalRatings = Integer.parseInt(split[0]);

			try (FileWriter out = new FileWriter("TopTenUsers.txt", true)) {
				String first = i + 1 + ") " + userID;
				String second = "Number of Ratings: " + totalRatings;
				out.write(String.format("%-30s%s%n", first, second));
			}
		}
		System.out.println("Top 10 Users calculated in TopTenUsers.txt");
	}

}