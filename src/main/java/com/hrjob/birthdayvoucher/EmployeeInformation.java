package com.hrjob.birthdayvoucher;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EmployeeInformation implements WritableComparable<EmployeeInformation> {
    private Text employeeName;
    private IntWritable employeeId;
    private IntWritable dayOfMonth;
    
    public Text getEmployeeName() {
        return employeeName;
    }
    
    public void setEmployeeName(Text employeeName) {
        this.employeeName = employeeName;
    }
    
    public IntWritable getEmployeeId() {
        return employeeId;
    }
    
    public void setEmployeeId(IntWritable employeeId) {
        this.employeeId = employeeId;
    }
    
    public IntWritable getDayOfMonth() {
        return dayOfMonth;
    }
    
    public void setDayOfMonth(IntWritable dayOfMonth) {
        this.dayOfMonth = dayOfMonth;
    }
    
    public EmployeeInformation() {
        setEmployeeName(new Text());
        setDayOfMonth(new IntWritable());
        setEmployeeId(new IntWritable());
    }
    
    public EmployeeInformation(Text employeeName, IntWritable employeeId, IntWritable dayOfMonth) {
        setEmployeeName(employeeName);
        setEmployeeId(employeeId);
        setDayOfMonth(dayOfMonth);
    }
    
    @Override
    public String toString() {
        return employeeName + " " + employeeId + " " + dayOfMonth;
    }
    
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        employeeName.write(dataOutput);
        employeeId.write(dataOutput);
        dayOfMonth.write(dataOutput);
    }
    
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        employeeName.readFields(dataInput);
        employeeId.readFields(dataInput);
        dayOfMonth.readFields(dataInput);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        return (employeeName.hashCode() + employeeId.hashCode() + dayOfMonth.hashCode()) * prime;
    }
    
    @Override
    public boolean equals(Object o) {
       if (o == this) {
           return true;
       }
       
       if (o == null || o.getClass() != this.getClass()) {
           return false;
       }
       
       EmployeeInformation other = (EmployeeInformation) o;
       
       return employeeId == other.employeeId
               && (other.employeeName != null && other.employeeName == this.employeeName)
               && (other.employeeId != null && other.employeeId == this.employeeId);
    }
    
    @Override
    public int compareTo(EmployeeInformation employeeInformation) {
        int compare = employeeName.compareTo(employeeInformation.employeeName);
        if (compare != 0)  return 0;
        
        if (employeeId != employeeInformation.employeeId || dayOfMonth != employeeInformation.dayOfMonth) {
            return 0;
        }
        return  1;
    }
}
