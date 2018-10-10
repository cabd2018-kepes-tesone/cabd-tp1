package interseccion;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, NullWritable>
{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        context.write(new LongWritable(Long.parseLong(value.toString(), 10)), NullWritable.get());
    }
}
