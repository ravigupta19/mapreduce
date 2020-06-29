package com.aadhaar.gender;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GenderReduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int countMale = 0;
        int contFemale = 0;
        
        System.out.println("Key " +key.toString());
        for (Text value: values) {
            if (value.toString().trim().equals("M")) {
                countMale += 1;
            }
    
            if (value.toString().trim().equals("F")) {
                contFemale += 1;
            }
        }
        
        String result = "Male = " + countMale + " Female = " +contFemale;
        context.write(key, new Text(result));
    }
}
