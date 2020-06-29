package com.wordcountaverage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AverageMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private boolean isDigit(String str) {
        char firstChar = str.charAt(0);
    
        return (firstChar >= '0' && firstChar <= '9');
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        for (String token: tokens) {
            token = token.toLowerCase().trim();
            if (token.length() > 0 && isDigit(token) == false) {
                char firstChar = token.charAt(0);
                context.write(new Text(String.valueOf(firstChar)), new IntWritable(token.length()));
            }
        }
    }
}
