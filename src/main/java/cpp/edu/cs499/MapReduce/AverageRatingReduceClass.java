package cpp.edu.cs499.MapReduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageRatingReduceClass extends
		Reducer<Text, FloatWritable, Text, FloatWritable> {

	@Override
	protected void reduce(Text text, Iterable<FloatWritable> values,
			Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {

		int count = 0;
		float sum = 0;
		float average = 0;
		
		Iterator<FloatWritable> i = values.iterator();
		while (i.hasNext()) {
			sum += i.next().get();
			count++;
		}
		
		average = sum / count;
		context.write(text, new FloatWritable(average));
	}

}