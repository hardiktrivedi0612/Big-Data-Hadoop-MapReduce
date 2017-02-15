import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
public class TopTenRated {

	public static class TopTenRatedMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
		private Text businessId = new Text();
		private FloatWritable stars = new FloatWritable();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			String recordValues[] = value.toString().split("::");
			String businessIdStr = recordValues[2];
			Float starsVal = Float.parseFloat(recordValues[3]);
			businessId.set(businessIdStr);
			stars.set(starsVal);
			context.write(businessId, stars);
		}
	}

	public static class TopTenRatedReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		private static TreeSet<TotalAndCountWritable> topRatedSet = new TreeSet<>(new TotalAndCountComparator());

		@Override
		protected void reduce(Text key, Iterable<FloatWritable> values,
				Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			TotalAndCountWritable totalAndCount = new TotalAndCountWritable();
			for (FloatWritable value : values) {
				totalAndCount.getTotal().set(
						(Float.parseFloat(totalAndCount.getTotal().toString()) + Float.parseFloat(value.toString())));
				totalAndCount.getCount().set((Long.parseLong(totalAndCount.getCount().toString()) + 1));
			}
			totalAndCount.getText().set(key);
			if (topRatedSet.size() < 10) {
				topRatedSet.add(totalAndCount);
			} else {
				Float avg1 = new Float((Float.parseFloat(totalAndCount.getTotal().toString())
						/ Float.parseFloat(totalAndCount.getCount().toString())));
				Float avg2 = new Float((Float.parseFloat(topRatedSet.last().getTotal().toString())
						/ Float.parseFloat(topRatedSet.last().getCount().toString())));
				if (avg2 < avg1) {
					Iterator<TotalAndCountWritable> iter = topRatedSet.iterator();
					int i = 0;
					while (i < 10) {
						iter.next();
						i++;
					}
					iter.remove();
					topRatedSet.add(totalAndCount);
				}
			}
		}

		@Override
		protected void cleanup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new Text("Tree Map Size = "), new FloatWritable(topRatedSet.size()));
			for (TotalAndCountWritable key : topRatedSet) {
				context.write(key.getText(), new FloatWritable(
						(Float.parseFloat(key.getTotal().toString()) / Float.parseFloat(key.getCount().toString()))));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TopTenRated <in> <out>");
			System.exit(2);
		}
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		Job job = Job.getInstance(conf, "Top Ten Rated");
		job.setJarByClass(TopTenRated.class);
		job.setMapperClass(TopTenRatedMapper.class);
		job.setReducerClass(TopTenRatedReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

class TotalAndCountWritable implements Writable {
	private Text text;
	private FloatWritable total;
	private LongWritable count;

	@Override
	public String toString() {
		return "TotalAndCountWritable [text=" + text + ", total=" + total + ", count=" + count + "]";
	}

	public TotalAndCountWritable() {
		total = new FloatWritable();
		total.set(0);
		count = new LongWritable();
		count.set(0);
		text = new Text();
	}

	public Text getText() {
		return text;
	}

	public void setText(Text text) {
		this.text = text;
	}

	public FloatWritable getTotal() {
		return total;
	}

	public void setTotal(FloatWritable total) {
		this.total = total;
	}

	public LongWritable getCount() {
		return count;
	}

	public void setCount(LongWritable count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		total.readFields(in);
		count.readFields(in);
		text.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		total.write(out);
		count.write(out);
		text.write(out);
	}

}

class TotalAndCountComparator implements Comparator<TotalAndCountWritable> {

	@Override
	public int compare(TotalAndCountWritable o1, TotalAndCountWritable o2) {
		Float avg1 = new Float(
				(Float.parseFloat(o1.getTotal().toString()) / Float.parseFloat(o1.getCount().toString())));
		Float avg2 = new Float(
				(Float.parseFloat(o2.getTotal().toString()) / Float.parseFloat(o2.getCount().toString())));
		if (avg1 <= avg2) {
			return 1;
		}
		return -1;
	}
}
