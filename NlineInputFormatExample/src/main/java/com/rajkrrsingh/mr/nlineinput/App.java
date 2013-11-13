package com.rajkrrsingh.mr.nlineinput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hello world!
 *
 */
public class App extends Configured implements Tool
{
    public static void main( String[] args ) throws Exception
    {
        int returnCode = ToolRunner.run(new Configuration(), new App(), args);
        System.exit(returnCode);
    }

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out
					.printf("Two parameters are required for App <input dir> <output dir>\n");
			return -1;
		}
		Job job = new Job(getConf());
		job.setJobName("NlineInputFormat Example");
		job.setJarByClass(App.class);
		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job, new Path(args[0]));
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 20);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(NlineInputFormatMapper.class);
		job.setNumReduceTasks(0);
 
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
}
