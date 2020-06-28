package com.hrjob.birthdayvoucher;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class BirthdayVoucherMap extends Mapper<LongWritable, Text, EmployeeInformation, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        
        if (key.get() > 0) {
            if (tokens.length > 3) {
                String dob = tokens[13].trim();
                String dateOfTermination = tokens[21].trim();
                String nameOfEmployee = tokens[0].trim() + "," +tokens[1].trim();
                String employeeId = tokens[2].trim();
    
                if (dateOfTermination == null || dateOfTermination.length() == 0) {
                    String[] splitsDob = dob.split("/");
                    int month = new Integer(splitsDob[0]);
                    String date = splitsDob[1];
                    if (month >= 1 && month <= 12) {
                        EmployeeInformation info = new EmployeeInformation();
                        info.setEmployeeName(new Text(nameOfEmployee));
                        info.setEmployeeId(new IntWritable(new Integer(employeeId)));
                        info.setDayOfMonth(new IntWritable(new Integer(date)));
                        context.write(info, new IntWritable(month));
                    }
                }
            }
        }
    }
}
