package ejercicio8;

import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<LongWritable, NullWritable, LongWritable, NullWritable>
{
    public void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
    {
        Iterator<NullWritable> iterator = values.iterator();

        iterator.next();

        // si está en un solo conjunto, está en X
        if (!iterator.hasNext()) {
            context.write(key, NullWritable.get());
        }
    }
}
