package njucs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Sort extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Sort(), args);
		System.exit(exitCode);
	}

	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		String[] args = new GenericOptionsParser(conf, arg0).getRemainingArgs();
		if (args.length != 2) {
			System.err.println("Usage:seg.Sort <input> <output>");
			System.exit(2);
		}
		Job job = new Job(conf, "sort.jar");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1])))
			fs.delete(new Path(args[1]), true);
		job.setJarByClass(Sort.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		job.setSortComparatorClass(DescComparator.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String str[] = value.toString().split("\t");
			context.write(new IntWritable(Integer.valueOf(str[1])), new Text(str[0]));
		}
	}

	public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
		static int sortCount = 1;

		private Text result = new Text();

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				if (sortCount > 100) {
					break;
				}
				result.set(Integer.toString(sortCount) + ": " + val + ", ");
				context.write(result, key);
				sortCount = sortCount + 1;
			}
		}
	}

	public static class DescComparator extends WritableComparator {

		protected DescComparator() {
			super(IntWritable.class, true);
		}

		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
			return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);
		}

		@Override
		public int compare(Object a, Object b) {
			return -super.compare(a, b);
		}
	}
}
