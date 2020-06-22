package com.wordcomatrix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordcoReducer extends Reducer<TextPair, Iterable<IntWritable>, TextPair, IntWritable> {
    
    protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value: values) {
            sum += value.get();
        }
        
        context.write(key, new IntWritable(sum));
    }
}
