package cardinalidad;

import java.io.IOException;
import java.lang.Long;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, LongWritable>
{
    private static LongWritable uno = new LongWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        context.write(uno, uno);
    }
}
