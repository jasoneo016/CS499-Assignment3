package cpp.edu.cs499.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortUserRatingsMapClass extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	private Text users = new Text();
		
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		String[] split = value.toString().split("\\s+");
		String userID = split[0];
		IntWritable totalRatings = new IntWritable(Integer.parseInt(split[1]));

		users.set(userID);
		context.write(totalRatings, users);
	}
}