package igualdad;

import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<LongWritable, Text, LongWritable, Text>
{
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        Iterator<Text> iterator = values.iterator();

        // obtengo el primer elemento, proveniente de un conjunto
        iterator.next();

        // si no está en el otro conjunto, son distintos
        if (!iterator.hasNext()) {
            context.write(key, new Text("NO"));
        }
    }
}
