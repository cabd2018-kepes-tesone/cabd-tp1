package interseccion;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<LongWritable, NullWritable, LongWritable, NullWritable>
{
    public void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
    {
        Iterator<NullWritable> iterator = values.iterator();

        // obtengo el primer null y avanzo
        iterator.next();

        // si hay un segundo null es porque el elemento está en los dos conjuntos
        if (iterator.hasNext()) {
            context.write(key, NullWritable.get());
        }
    }
}
