package pertenencia;

import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<LongWritable, Text, LongWritable, Text>
{
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        Iterator<Text> iterator = values.iterator();

        // si el primer elemento viene de E, se fija si está en A
        if (iterator.next().toString().equals("E")) {
            context.write(key, new Text(iterator.hasNext() ? "SI" : "NO"));
        } else if (iterator.hasNext()) { // en cambio, si viene de A, se fija si está en E
            context.write(key, new Text("SI"));
        }
    }
}
