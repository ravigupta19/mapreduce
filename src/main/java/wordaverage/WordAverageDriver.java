package wordaverage;

import com.wordcomatrix.WordcoDriver;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordAverageDriver extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new WordAverageDriver(), args);
        System.exit(result);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Average of Word");
        job.setJarByClass(WordcoDriver.class);
        job.setMapperClass(WordAverageMapper.class);
        job.setCombinerClass(WordAverageCombiner.class);
        job.setReducerClass(WordAverageReducer.class);
    
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setOutputKeyClass();
        job.setCombinerClass();
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
