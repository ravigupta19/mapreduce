package com.aadhaar.gender;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GenderMap extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length >= 6)  {
            String state = tokens[2].trim();
            String gender = tokens[6].trim();
            
            context.write(new Text(state), new Text(gender));
        }
    }
}
