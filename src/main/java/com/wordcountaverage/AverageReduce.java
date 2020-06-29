package com.wordcountaverage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int numOfWords = 0;
        int sumOfWordsLength = 0;
    
        for (IntWritable value: values) {
            sumOfWordsLength += value.get();
            numOfWords += 1;
        }
        
        float average = sumOfWordsLength / numOfWords;
        int intAverage = Math.round(average);
        
        context.write(key, new IntWritable(intAverage));
        
    }
}
