import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author Hardik
 *
 */
public class UniqueCategories {

	public static class UniqueCategoriesMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		private Text category = new Text();
		private NullWritable nullValue = NullWritable.get();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String recordValues[] = value.toString().split("::");
			String fullAddress = recordValues[1];
			if (fullAddress.contains("Palo Alto")) {
				String[] categories = recordValues[2].trim().substring(5, recordValues[2].length() - 1).split(",");
				for (String categoryStr : categories) {
					if (!categoryStr.equalsIgnoreCase("")) {
						category.set(categoryStr.trim());
						context.write(category, nullValue);
					}
				}
			}
		}
	}

	public static class UniqueCategoriesReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		private NullWritable nullValue = NullWritable.get();

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, nullValue);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: UniqueCategories <in> <out>");
			System.exit(2);
		}
		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(otherArgs[1]);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		Job job = Job.getInstance(conf, "Unique Categories");
		job.setJarByClass(UniqueCategories.class);
		job.setMapperClass(UniqueCategoriesMapper.class);
		job.setReducerClass(UniqueCategoriesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
