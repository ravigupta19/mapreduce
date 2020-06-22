package com.wordcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
    
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new WordCount(), args);
        System.exit(res);
    }
    

    public int run(String[] args) throws Exception {
        // Creating job object
        Job job = Job.getInstance(getConf(), "Word Count");
        
        // Setting the Driver class
        job.setJarByClass(this.getClass());
        
        // Setting InputFile
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        
        // Setting Output directory
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // Setting Mapper Class
        job.setMapperClass(WordCountMapper.class);
        
        // Setting Reducer Class
        job.setReducerClass(WordCountReducer.class);
        
        // Setting Reducer Output Key Class
        job.setOutputKeyClass(Text.class);
        
        // Setting Reducer Output Value Class
        job.setMapOutputValueClass(IntWritable.class);
        
        // Waiting for job completion
        return job.waitForCompletion(true) ? 0 : 1;
    }
}


