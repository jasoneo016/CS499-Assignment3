package cpp.edu.cs499.MapReduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	private String temp;

	//Top 10 Movies that have the highest average ratings
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String first = value.toString().split(",")[0];
		String last = value.toString().split(",")[2];
		
		System.out.println(first);
		
		/*
		 * int count = 0;
		 * while temp = first {
		 * count++;
		 * double ratings = 0;
		 * double ratings += last.toInteger();
		 * 
		 * if (temp != first) {
		 * temp = first;
		 * 
		 * 
		 * ratings / count;
		 */
		
		word.set(first);
		context.write(word, one);
	}
}