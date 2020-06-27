package com.wordaverage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordAverageCombiner extends Reducer<IntWritable, LengthCountPair, IntWritable, LengthCountPair> {
    
    private IntWritable one = new IntWritable(1);
    
    @Override
    protected void reduce(IntWritable key, Iterable<LengthCountPair> values, Context context) throws IOException, InterruptedException {
        int sumNumberOfWords = 0;
        int sumLengthOfWords = 0;
        
        for (LengthCountPair value: values) {
            sumNumberOfWords += value.getNumberOfWords().get();
            sumLengthOfWords += value.getLengthOfWords().get();
        }
        context.write(one, new LengthCountPair(sumNumberOfWords, sumLengthOfWords));
    }
}
