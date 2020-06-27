package com.wordaverage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LengthCountPair implements WritableComparable<LengthCountPair> {
    
    private IntWritable numberOfWords;
    private IntWritable lengthOfWords;
    
    public IntWritable getNumberOfWords() {
        return numberOfWords;
    }
    
    public void setNumberOfWords(IntWritable numberOfWords) {
        this.numberOfWords = numberOfWords;
    }
    
    public IntWritable getLengthOfWords() {
        return lengthOfWords;
    }
    
    public void setLengthOfWords(IntWritable lengthOfWords) {
        this.lengthOfWords = lengthOfWords;
    }
    
    public LengthCountPair(IntWritable numberOfWords, IntWritable lengthOfWords) {
        setNumberOfWords(numberOfWords);
        setLengthOfWords(lengthOfWords);
    }
    
    public LengthCountPair() {
        setLengthOfWords(new IntWritable());
        setNumberOfWords(new IntWritable());
    }
    
    public LengthCountPair(int numberOfWords, int lengthOfWords) {
        setNumberOfWords(new IntWritable(numberOfWords));
        setLengthOfWords(new IntWritable(lengthOfWords));
    }
    
    @Override
    public int compareTo(LengthCountPair lengthCountPair) {
        return 0;
    }
    
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        numberOfWords.write(dataOutput);
        lengthOfWords.write(dataOutput);
    }
    
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        numberOfWords.readFields(dataInput);
        lengthOfWords.readFields(dataInput);
    }
    
    @Override
    public int hashCode() {
        return this.lengthOfWords.get() * this.numberOfWords.get() % 11;
    }
    
    @Override
    public boolean equals(Object o) {
        
        return super.equals(o);
    }
    
    @Override
    public String toString() {
        return this.numberOfWords + " " + this.lengthOfWords;
    }
}
