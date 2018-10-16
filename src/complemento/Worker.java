package complemento;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class Worker extends Configured implements Tool
{
    String baseDir;

    private Job setupJob(String[] args) throws IOException
    {
        Configuration conf = getConf();

        Job job = new Job(conf, "Complemento");

        job.setJarByClass(Worker.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // configure Reducer
        job.setReducerClass(Reducer.class);

        // configure output
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);

        // configure input and output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // configure output folder
        FileSystem fs = FileSystem.get(conf);
        String outputDir = "output";
        if (fs.exists(new Path(outputDir))) {
            fs.delete(new Path(outputDir), true);
        }

        // configure Mappers
        MultipleInputs.addInputPath(job, new Path("inputA"), TextInputFormat.class, AMapper.class);
        MultipleInputs.addInputPath(job, new Path("inputU"), TextInputFormat.class, UMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        return job;
    }

    @Override
    public int run(String[] args) throws Exception
    {
        Job job = setupJob(args);

        boolean success = job.waitForCompletion(true);

        if (!success) {
            System.out.println("Error job");

            return -1;
        }

        return 0;
    }
}
