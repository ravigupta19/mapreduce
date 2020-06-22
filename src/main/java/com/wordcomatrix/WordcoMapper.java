package com.wordcomatrix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordcoMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
    
    private IntWritable one = new IntWritable(1);
    private TextPair wordPair = new TextPair();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int neighbors  = context.getConfiguration().getInt("neighbors", 2);
       String[] tokens = value.toString().split(" ");
       
       if (tokens.length > 1) {
           for (int i = 0; i < tokens.length; i++) {
               wordPair.setFirst(new Text(tokens[i]));
    
               int start = (i - neighbors < 0) ? 0 : i - neighbors;
               int end = (i + neighbors >= tokens.length) ? tokens.length - 1  : i + neighbors;
    
               for (int j = start; j <= end; j++) {
                   if (j == i) {
                       continue;
                   }
                   wordPair.setSecond(new Text(tokens[j]));
                   context.write(wordPair, one);
               }
           }
       }
    }
}
