package cardinalidad;

import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<LongWritable, LongWritable, LongWritable, LongWritable>
{
    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
    {
        long cantidad = 0;

        Iterator<LongWritable> iterator = values.iterator();

        while (iterator.hasNext()) {
            cantidad += iterator.next().get();
        }

        context.write(key, new LongWritable(cantidad));
    }
}
