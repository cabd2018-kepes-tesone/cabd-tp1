package ejercicio9;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

    Configuration conf;

    private Job setupJobX(String[] args) throws IOException
    {
        ejercicio8.Worker w = new ejercicio8.Worker();

        return w.setupJob(args, conf);
    }

    private Job setupJobCardinalidad(String [] args) throws IOException
    {
        cardinalidad.Worker w = new cardinalidad.Worker();

        return w.setupJob(args, conf);
    }

    private Job setupJobPertenencia(String [] args) throws IOException
    {
        pertenencia.Worker w = new pertenencia.Worker();

        return w.setupJob(args, conf);
    }

    @Override
    public int run(String[] args) throws Exception
    {
        conf = getConf();

        // abc x e pertencia cardinalidad
        Job job1 = setupJobX(new String[]{args[0], args[1]});
        Job job2 = setupJobPertenencia(new String[]{args[1], args[2], args[3]});
        Job job3 = setupJobCardinalidad(new String[]{args[1], args[4]});

        boolean success1 = job1.waitForCompletion(true);
        boolean success2 = job2.waitForCompletion(true);
        boolean success3 = job3.waitForCompletion(true);

        if (!success1 || !success2 || !success3) {
            System.out.println("Error job");

            return -1;
        }

        return 0;
    }
}
