package com.wordaverage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordAverageMapper extends Mapper<LongWritable, Text, IntWritable, LengthCountPair> {
    private IntWritable one = new IntWritable(1);
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s");
        for (String token: tokens) {
            token = token.trim().replaceAll("[^a-zA-Z0-9]", "");
            if(token.length() > 0) {
                context.write(one, new LengthCountPair(1, token.length()));
            }
        }
    }
}
