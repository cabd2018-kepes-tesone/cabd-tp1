package pertenencia;

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
        return setupJob(args, getConf());
    }

    public Job setupJob(String[] args, Configuration conf) throws IOException
    {
        Job job = new Job(conf, "Complemento");

        job.setJarByClass(Worker.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // configure Reducer
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // configure input and output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // configure output folder
        FileSystem fs = FileSystem.get(conf);
        String outputDir = args[2];
        if (fs.exists(new Path(outputDir))) {
            fs.delete(new Path(outputDir), true);
        }

        // configure Mappers
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, EMapper.class);

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
