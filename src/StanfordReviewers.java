import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StanfordReviewers {
	public static final String DELIMITER = "::";
	public static final String CITY_NAME = "Stanford";
	public static final String FILE_URL_KEY = "FILE_URL";

	public static class ReviewMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static HashSet<String> businessIds = new HashSet<>();
		private Text businessId = new Text();
		private Text userIdAndRating = new Text();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Path businessFilePath = new Path(conf.get(FILE_URL_KEY));
			FileSystem fs = FileSystem.get(conf);
			System.out.println(fs.exists(businessFilePath));
			try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(businessFilePath)))) {
				String currentLine;
				while ((currentLine = bufferedReader.readLine()) != null) {
					String[] values = currentLine.split(DELIMITER);
					if (values[1].contains(CITY_NAME)) {
						businessIds.add(values[0]);
					}
				}
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String recordValues[] = value.toString().split(DELIMITER);
			if (businessIds.contains(recordValues[2])) {
				businessId.set(recordValues[2]);
				userIdAndRating.set(recordValues[1] + DELIMITER + recordValues[3]);
				context.write(businessId, userIdAndRating);
			}
		}
	}

	public static class ReviewReducer extends Reducer<Text, Text, Text, Text> {
		private Text userId = new Text();
		private Text rating = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				String[] recordValues = value.toString().split(DELIMITER);
				userId.set(recordValues[0]);
				rating.set(recordValues[1]);
				context.write(userId, rating);
			}
		}
	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: TopTenRated <review-input> <local-business-input> <output>");
			System.exit(2);
		}
		Path outputPath = new Path(otherArgs[2]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		conf.set(FILE_URL_KEY, otherArgs[1]);
		Job job = Job.getInstance(conf, "StanfordReviewers");
		job.setJarByClass(StanfordReviewers.class);
		job.setMapperClass(ReviewMapper.class);
		job.setReducerClass(ReviewReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
