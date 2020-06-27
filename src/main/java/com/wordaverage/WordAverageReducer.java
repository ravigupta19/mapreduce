package com.wordaverage;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordAverageReducer extends Reducer<IntWritable, LengthCountPair, IntWritable, DoubleWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<LengthCountPair> values, Context context) throws IOException, InterruptedException {
        int sumNumberOfWords = 0;
        int sumLengthOfWords = 0;
    
        for (LengthCountPair value: values) {
            sumNumberOfWords += value.getNumberOfWords().get();
            sumLengthOfWords += value.getLengthOfWords().get();
        }
        double average = sumLengthOfWords / sumNumberOfWords;
        context.write(new IntWritable(sumLengthOfWords), new DoubleWritable(average));
    }
}
