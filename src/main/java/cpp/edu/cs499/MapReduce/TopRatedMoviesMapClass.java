package cpp.edu.cs499.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopRatedMoviesMapClass extends
		Mapper<LongWritable, Text, FloatWritable, Text> {

	private Text movies = new Text();
		
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, FloatWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		String[] split = value.toString().split("\\s+");
		String movieID = split[0];
		FloatWritable averageRating = new FloatWritable(Float.parseFloat(split[1]));

		movies.set(movieID);
		context.write(averageRating, movies);
	}
}