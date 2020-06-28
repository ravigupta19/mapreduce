package com.hrjob.birthdayvoucher;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BirthdayVoucherDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new BirthdayVoucherDriver(), args);
        System.exit(result);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        
        job.setJobName("Birthday Voucher Job");
        job.setJarByClass(BirthdayVoucherDriver.class);
        job.setMapperClass(BirthdayVoucherMap.class);
        job.setReducerClass(BirthdayVoucherReducer.class);
        job.setPartitionerClass(BirthdayVoucherPartitioner.class);
        job.setMapOutputKeyClass(EmployeeInformation.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(EmployeeInformation.class);
        job.setNumReduceTasks(12);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
