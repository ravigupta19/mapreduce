package com.hrjob.birthdayvoucher;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BirthdayVoucherReducer extends Reducer<EmployeeInformation, IntWritable, IntWritable, EmployeeInformation> {
    @Override
    protected void reduce(EmployeeInformation key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value: values) {
            context.write(value, key);
        }
    }
}
