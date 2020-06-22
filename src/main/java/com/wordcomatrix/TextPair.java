package com.wordcomatrix;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {
    private Text first;
    private Text second;
    
    public TextPair() {
        setFirst(new Text());
        setSecond(new Text());
    }
    
    public TextPair(Text first, Text second) {
        setFirst(first);
        setSecond(second);
    }
    
    public TextPair(String first, String second) {
        setFirst(new Text(first));
        setSecond(new Text(second));
    }
    
    public void setFirst(Text first) {
        this.first = first;
    }
    
    public void setSecond(Text second) {
        this.second = second;
    }
    
    public Text getFirst() {
        return first;
    }
    
    public Text getSecond() {
        return second;
    }
    
    @Override
    public String toString() {
        return this.first + " " + this.second;
    }
    
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }
    
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }
    
    @Override
    public int compareTo(TextPair textPair) {
        int cmp = first.compareTo(textPair.first);
        
        if (cmp != 0) {
            return cmp;
        }
        
        return second.compareTo(textPair.second);
    }
    
    @Override
    public int hashCode() {
        return first.hashCode()*163 + second.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof TextPair) {
            TextPair tp = (TextPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return  false;
    }
}
