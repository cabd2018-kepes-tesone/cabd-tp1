package ejercicio8;

import java.io.IOException;
import java.lang.Long;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, NullWritable>
{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        LongWritable number = new LongWritable(Long.parseLong(value.toString(), 10));

        context.write(number, NullWritable.get());
    }
}
