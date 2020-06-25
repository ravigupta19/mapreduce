package wordaverage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordAverageMapper extends Mapper<LongWritable, Text, IntWritable, LengthCountPair> {

}
