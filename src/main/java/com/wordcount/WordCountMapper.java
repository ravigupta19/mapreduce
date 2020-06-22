package com.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String words[] = value.toString().split("\\s");
        Text currentWord = new Text();
        
        
        for (String word: words) {
            if (word.isEmpty()) {
                continue;
            }
            word = word.replaceAll("[^a-zA-Z]","").toLowerCase();
            currentWord = new Text(word);
            context.write(currentWord, one);
        }
    }
}
