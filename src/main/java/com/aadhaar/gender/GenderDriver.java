package com.aadhaar.gender;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class GenderDriver extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new GenderDriver(), args);
        System.exit(result);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        
        job.setJobName("Number of male and female in state");
        job.setJarByClass(GenderDriver.class);
        job.setMapperClass(GenderMap.class);
        job.setReducerClass(GenderReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
    
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        return job.waitForCompletion(true) ? 0: 1;
    }
}
