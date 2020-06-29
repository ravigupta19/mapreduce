package com.hrjob.married;

import com.hrjob.birthdayvoucher.EmployeeInformation;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MarriedMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    enum InfoCounter{MARRIED, FEMALE, TOTALEMPLOYEES};

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        
        if (key.get() > 0) {
            if (tokens.length > 3) {
                int is_married  = new Integer(tokens[3]);
                int is_female = new Integer(tokens[5]);
                String dateOfTermination = tokens[21].trim();
                if (dateOfTermination == null || dateOfTermination.length() == 0) {
                    if (is_married == 1) {
                        context.getCounter(InfoCounter.MARRIED).increment(1);
                    }
                    
                    if (is_female == 0) {
                        context.getCounter(InfoCounter.FEMALE).increment(1);
                    }
    
                    context.getCounter(InfoCounter.TOTALEMPLOYEES).increment(1);
                }
            }
        }
    }
}
