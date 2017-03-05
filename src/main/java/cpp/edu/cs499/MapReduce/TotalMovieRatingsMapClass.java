package cpp.edu.cs499.MapReduce;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalMovieRatingsMapClass extends
		Mapper<LongWritable, Text, Text, FloatWritable> {

	private Text movie = new Text();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {

		String movieID = value.toString().split(",")[0];
		FloatWritable rating = new FloatWritable(Float.parseFloat(value
				.toString().split(",")[2]));
		/*
		 * int count = 0; while temp = first { count++; double ratings = 0;
		 * double ratings += last.toInteger();
		 * 
		 * if (temp != first) { temp = first;
		 * 
		 * 
		 * ratings / count;
		 */

		movie.set(movieID);
		context.write(movie, rating);
	}
}