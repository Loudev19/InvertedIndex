package org.loudev;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

    private Text fileKey = new Text();
    private Text word = new Text();

    private Pattern pattern = Pattern.compile("www.[a-zA-Z0-9]+.com", Pattern.CASE_INSENSITIVE);
    private Matcher matcher;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit) context.getInputSplit();
      String filename = fileSplit.getPath().getName();

      StringTokenizer itr = new StringTokenizer(value.toString(), " '-");
      while (itr.hasMoreTokens()) {
        fileKey.set(filename);
        String cleanWord = itr.nextToken();
        matcher = pattern.matcher(cleanWord);

        if (!matcher.find()) {
          word.set(cleanWord.replaceAll("[^a-zA-ZáéíóúÁÉÍÓÚ]", "").toLowerCase());
        }
        context.write(word, fileKey);
      }
    }
  }

  public static class SaveReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> files, Context context) throws IOException, InterruptedException {
      Set<String> seenFiles = new HashSet<>();

      for (Text file : files) {
        String fileStr = file.toString();
        if (!seenFiles.contains(fileStr)) {
          context.write(key, file);
          seenFiles.add(fileStr);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: invertedindex <in> [<in>...] <out>");
      System.exit(2);
    }

    Job job = new Job(conf, "inverted index");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(SaveReducer.class);
    job.setReducerClass(SaveReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
