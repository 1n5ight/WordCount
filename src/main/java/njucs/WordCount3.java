package njucs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class WordCount3 {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    static enum CountersEnum {
      INPUT_WORDS
    }

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private boolean caseSensitive;
    private Set<String> patternsToSkip = new HashSet<String>();
    private Set<String> stopwordsToSkip = new HashSet<String>();

    private Configuration conf;
    private BufferedReader fis;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      conf = context.getConfiguration();
      caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
      // 分别是标点 和 停词
      if (conf.getBoolean("wordcount.skip.patterns", false)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        Path patternsPath = new Path(patternsURIs[0].getPath());
        String patternsFileName = patternsPath.getName().toString();
        parseSkipFile(patternsFileName);
      }
      if (conf.getBoolean("wordcount.skip.stopwords", false)) {
        URI[] stopwordsURIs = Job.getInstance(conf).getCacheFiles();
        Path stopwordsPath = new Path(stopwordsURIs[1].getPath());
        String stopwordsFileName = stopwordsPath.getName().toString();
        stopwordSkipFile(stopwordsFileName);
      }
    }

    private void parseSkipFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        // 要加入数字
        patternsToSkip.add("0");
        patternsToSkip.add("1");
        patternsToSkip.add("2");
        patternsToSkip.add("3");
        patternsToSkip.add("4");
        patternsToSkip.add("5");
        patternsToSkip.add("6");
        patternsToSkip.add("7");
        patternsToSkip.add("8");
        patternsToSkip.add("9");
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
      }
    }

    private void stopwordSkipFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String stopword = null;
        while ((stopword = fis.readLine()) != null) {
          stopwordsToSkip.add(stopword);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
      }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
      for (String pattern : patternsToSkip) {
        line = line.replaceAll(pattern, "");
      }
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        String tmp = itr.nextToken();
        // 去掉stopwords
        // 长度大于等于3
        if (tmp.length() >= 3 && !stopwordsToSkip.contains(tmp)) {
          word.set(tmp);
          context.write(word, one);
          Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.INPUT_WORDS.toString());
          counter.increment(1);
        }
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
      System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount3.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    List<String> otherArgs = new ArrayList<String>();
    // 分成两种skip
    for (int i = 0; i < remainingArgs.length; ++i) {
      if ("-skip1".equals(remainingArgs[i])) {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      } else if ("-skip2".equals(remainingArgs[i])) {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.getConfiguration().setBoolean("wordcount.skip.stopwords", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
    }

    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
