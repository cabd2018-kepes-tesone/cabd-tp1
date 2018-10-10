package igualdad;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class BMapper extends Mapper<LongWritable, Text, LongWritable, Text>
{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        LongWritable number = new LongWritable(Long.parseLong(value.toString(), 10));

        context.write(number, new Text("B"));
    }
}
