package edu.cmu;

import java.io.*;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.*;

public class InvertedIndexBonus {

    public static class Map extends Mapper<Object, Text, Text, Text> {
      private Text word = new Text();
      private Text filename = new Text();

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokenizer = value.toString().split("\\W+");
        FileSplit fs = (FileSplit) context.getInputSplit();
        String location = fs.getPath().getName();  
        filename.set(location);

	HashSet<String> hashset = new HashSet<String>();

        for (String s : tokenizer) {
	  if (hashset.contains(s)) {
	    continue;
          }
	  hashset.add(s);
          word.set(s);
          context.write(word, filename);
        }
      }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	HashSet<String> hashset = new HashSet<String>();
        StringBuffer buffer = new StringBuffer();
        for (Text value : values) {
	  String s = value.toString();
	  if (hashset.contains(s)) {
	    continue;
	  }
	  hashset.add(s);
          buffer.append(" "+value.toString());
        }
	
	Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	FileInputStream fileStream = new FileInputStream(cacheFiles[0].toString());
	BufferedReader reader = new BufferedReader(new InputStreamReader((InputStream) fileStream));
        String line = null;
	String keyString = key.toString();
        while ((line = reader.readLine()) != null) {
          if (line.equals(keyString)) {
	    return;
	  }
        }
        context.write(new Text(keyString+" :"), new Text(buffer.toString()));
      }
    }

    public static void main(String[] args) throws Exception {
      Job job = new Job();
      job.setJarByClass(edu.cmu.InvertedIndexBonus.class);
      job.setJobName("Inverted Index Bonus");

      DistributedCache.addCacheFile(new URI("hdfs:/english.stop.txt"), job.getConfiguration());

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.waitForCompletion(true);
    }
}

