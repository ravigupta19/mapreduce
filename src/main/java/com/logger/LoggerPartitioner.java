package com.logger;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class LoggerPartitioner extends Partitioner<Text, Text> implements Configurable {
    
    private Configuration configuration;
    
    private HashMap<String, Integer> months = new HashMap<String, Integer>();
    
    @Override
    public int getPartition(Text key, Text value, int numReducedTask) {
        return  months.get(value.toString());
    }
    
    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
        months.put("Jan", 0);
        months.put("Feb", 1);
        months.put("Mar", 2);
        months.put("Apr", 3);
        months.put("May", 4);
        months.put("Jun", 5);
        months.put("Jul", 6);
        months.put("Aug", 7);
        months.put("Sep", 8);
        months.put("Oct", 9);
        months.put("Nov", 10);
        months.put("Dec", 11);
    }
    
    @Override
    public Configuration getConf() {
        return this.configuration;
    }
}
