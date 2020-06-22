package com.wordcomatrix;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordcoDriver extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        int response = ToolRunner.run(new WordcoDriver(), args);
        System.out.println(response);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        
        job.setJobName("Word count co-occurrence matrix");
        
        job.setMapperClass(WordcoMapper.class);
        job.setReducerClass(WordcoReducer.class);
        // job.setCombinerClass(WordcoReducer.class);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(IntWritable.class);
    
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
        return job.waitForCompletion(true) ? 0 : 1;
       
    }
}
