package cpp.edu.cs499.MapReduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FloatWritableComparator<T> extends WritableComparator {

    protected FloatWritableComparator() {
        super(FloatWritable.class, true);
    }

    @SuppressWarnings("rawtypes")

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        FloatWritable f1 = (FloatWritable)w1;
        FloatWritable f2 = (FloatWritable)w2;

        return -1 * f1.compareTo(f2);
    }
}
