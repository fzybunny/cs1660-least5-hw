import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LeastN {

  public static final int N = 5;

  private static String[] toSortedKeyArray(Map<String, Integer> m)
  {
    Set<String> keyset = m.keySet();
    String[] sorted_words = keyset.toArray(new String[keyset.size()]);
    Arrays.sort(sorted_words);
    return sorted_words;
  }

  public static class MyMapper
    extends Mapper<Object, Text, Text, IntWritable>
  {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private Map<String, Integer> count_map;
    
    @Override
    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException
    {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
	      word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }


  public static class MyReducer
    extends Reducer<Text,IntWritable,Text,IntWritable>
  {
    private IntWritable result = new IntWritable();

    private Map<String, Integer> count_map;

    @Override
    public void setup(Context context)
      throws IOException, InterruptedException 
    { 
      count_map = new HashMap<>();
    } 

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
      throws IOException, InterruptedException
    {
      for (IntWritable v : values) {
        count_map.merge(key.toString(), v.get(), Integer::sum);
      }
    }

    @Override
    public void cleanup(Context context)
      throws IOException, InterruptedException
    {
      // Sort everything
      String[] sorted_words = toSortedKeyArray(count_map);

      // Output the 'top' N.
      Text        key   = new Text();
      IntWritable value = new IntWritable();

      int end = N < sorted_words.length ? N : sorted_words.length;
      for (int i = 0; i < end; i++) {
        key.set(sorted_words[i]);
        value.set(count_map.get(sorted_words[i]));
        context.write(key, value);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(LeastN.class);
    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
