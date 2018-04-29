//package hadoop;		// Remove the package line when sending to Hadoop

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BasicWordCount {

  public static class WCMapper					// Mapper class
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);		// Create a writable int with value 1
    private Text word = new Text();					// Text object to hold words read

    public void map(Object key, Text value, Context context		// Map function: key is bytes so far, value is line from file
                    ) throws IOException, InterruptedException {
      Scanner itr = new Scanner(value.toString());	// Scanner for line from file
      while (itr.hasNext()) {
        word.set(itr.next());					// Set word to next word in line
        context.write(word, one);					// Write, i.e. emit word as key and 1 as value (IntWritable(1))
     //System.out.printf("%s : %s\n",word.toString(),one.toString()); 
     }
    }
  }

  public static class WCReducer					// Reducer class
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();			// Prepare writeable int for result

    public void reduce(Text key, Iterable<IntWritable> values,		// Reduce function: key is word as emitted by map function and values is array of values assocaited with each key
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();						// Iterate values for this key and sum them (essentially add all the 1s, so count, except that it might be called more than once (or combined), so must be sum, not ++1)
      }
      result.set(sum);							// Set value of result to sum
      context.write(key, result);					// Emit key and result (i.e word and count).
    }
  }

  public static void main(String[] args) throws Exception {		// Main program to run
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(BasicWordCount.class);
    job.setMapperClass(WCMapper.class);				// Set mapper class to WCMapper defined above
    job.setCombinerClass(WCReducer.class);				// Set combine class to WCReducer defined above
    job.setReducerClass(WCReducer.class);				// Set reduce class to WCReducer defined above
    job.setOutputKeyClass(Text.class);					// Class of output key is Text
    job.setOutputValueClass(IntWritable.class);				// Class of output value is IntWritable
    FileInputFormat.addInputPath(job, new Path(args[0]));		// Input path is first argument when program called
    FileOutputFormat.setOutputPath(job, new Path(args[1]));		// Output path is second argument when program called
    job.waitForCompletion(true);	   		// waitForCompletion submits the job and waits for it to complete, parameter is verbose.
    Counters counters=job.getCounters();
    System.out.println("Input Records: "+counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
  }
}




