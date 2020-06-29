package com.hrjob.married;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MarriredDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new MarriredDriver(), args);
        System.exit(result);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        
        job.setJobName("Number of Married Person");
        job.setJarByClass(MarriredDriver.class);
        job.setMapperClass(MarriedMapper.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setNumReduceTasks(0);
    
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        int success = job.waitForCompletion(true)  ? 0 : 1;
        
        if (success == 0) {
            long married = job.getCounters().findCounter(MarriedMapper.InfoCounter.MARRIED).getValue();
            long female = job.getCounters().findCounter(MarriedMapper.InfoCounter.FEMALE).getValue();
            long totalEmployee = job.getCounters().findCounter(MarriedMapper.InfoCounter.TOTALEMPLOYEES).getValue();
            System.out.println("married   = " + married);
            System.out.println("female   = " + female);
            System.out.println("Total   = " + totalEmployee);
            return 0;
        }
        return  1;
    }
}
