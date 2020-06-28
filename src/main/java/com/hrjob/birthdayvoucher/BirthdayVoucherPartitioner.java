package com.hrjob.birthdayvoucher;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class BirthdayVoucherPartitioner extends Partitioner<EmployeeInformation, IntWritable> implements Configurable {
    private Configuration configuration;
    
    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
        
    }
    
    @Override
    public Configuration getConf() {
        return configuration;
    }
    
    @Override
    public int getPartition(EmployeeInformation employeeInformation, IntWritable value, int numReduceTask) {
        return value.get() - 1;
    }
}
