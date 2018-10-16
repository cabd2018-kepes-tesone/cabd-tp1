package diferencia;

import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<LongWritable, Text, LongWritable, NullWritable>
{
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        Iterator<Text> iterator = values.iterator();

        // si el primer elemento viene de A y no está en B, está en la diferencia
        if (iterator.next().toString().equals("A") && !iterator.hasNext()) {
            context.write(key, NullWritable.get());
        }
    }
}
