package session_3_assignments_2;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class totalCount {
	public static class TokenizerMapper

	extends Mapper<Object, Text, Text, IntWritable>{



		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();



		public void map(Object key, Text value, Context context

				) throws IOException, InterruptedException {

			String line = value.toString();

			if (line.contains("NA")) 
			{
				System.out.println("Invalid data");
			}
			else
			{
				String[] lineSpilt= (value.toString()).split("|");
				String CompanyName=lineSpilt[0];
				word.set(CompanyName);
				context.write(word,one);
			}
		}
	}
	public static class IntSumReducer

	extends Reducer<Text,IntWritable,Text,IntWritable> {

		private IntWritable result = new IntWritable();



		public void reduce(Text key, Iterable<IntWritable> values,

				Context context

				) throws IOException, InterruptedException {

			int sum = 0;

			for (IntWritable val : values) {

				sum += val.get();

			}

			result.set(sum);

			context.write(key, result);

		}
	}
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

	    Job job = Job.getInstance(conf, "Total Units");

	    job.setJarByClass(totalCount.class);

	    job.setMapperClass(TokenizerMapper.class);

	    job.setReducerClass(IntSumReducer.class);

	    job.setOutputKeyClass(Text.class);

	    job.setOutputValueClass(IntWritable.class);
	    Path outputPath= new Path(args[1]);

	    FileInputFormat.addInputPath(job, new Path(args[0]));

	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    outputPath.getFileSystem(conf).delete(outputPath);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
