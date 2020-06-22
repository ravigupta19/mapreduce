package com.logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class LoggerMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    public static List<String> months = Arrays.asList("Jan", "Feb", "Mar", "Apr", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec");
    // 199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] listSplitValue = value.toString().split(" ");
        System.out.println(listSplitValue.length);
        if (listSplitValue.length > 3) {
            
            String ipAddressOrHostName = listSplitValue[0];
            String[] dateFields = listSplitValue[3].split("/");
            
            System.out.println(ipAddressOrHostName);
            System.out.println(dateFields.length);
            if (dateFields.length > 1) {
            String month = dateFields[1];
            
            if (months.contains(month)) {
                System.out.println(month);
                context.write(new Text(ipAddressOrHostName), new Text(month));
            }
        }
        }
    }
}

