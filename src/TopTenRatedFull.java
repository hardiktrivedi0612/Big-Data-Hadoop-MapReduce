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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopTenRatedFull {

	public static enum TYPES {
		RATING("rating"), DETAILS("details");

		public final String value;

		TYPES(final String value) {
			this.value = value;
		}
	}

	public static class ReviewMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
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

	public static class AvgReviewReducer extends Reducer<Text, FloatWritable, Text, NullWritable> {
		private static TreeSet<TotalAndCountWritable> topRatedSet = new TreeSet<>(new TotalAndCountComparator());
		private BusinessObject object = new BusinessObject();

		@Override
		protected void reduce(Text key, Iterable<FloatWritable> values,
				Reducer<Text, FloatWritable, Text, NullWritable>.Context context)
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
		protected void cleanup(Reducer<Text, FloatWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (TotalAndCountWritable key : topRatedSet) {
				object.getRating().set(
						(Float.parseFloat(key.getTotal().toString()) / Float.parseFloat(key.getCount().toString())));
				context.write(new Text(key.getText().toString() + "::" + object.getRating().toString()),
						NullWritable.get());
			}
		}
	}

	public static class ReviewUtilMapper extends Mapper<LongWritable, Text, Text, BusinessObject> {

		private Text businessId = new Text();
		private BusinessObject object = new BusinessObject();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, BusinessObject>.Context context)
				throws IOException, InterruptedException {
			String recordValues[] = value.toString().split("::");
			String businessIdStr = recordValues[0];
			Float avgVal = Float.parseFloat(recordValues[1]);
			businessId.set(businessIdStr);
			object.getRating().set(avgVal);
			object.getType().set(TYPES.RATING.value);
			context.write(businessId, object);
		}

	}

	public static class DetailsMapper extends Mapper<LongWritable, Text, Text, BusinessObject> {
		private Text businessId = new Text();
		private BusinessObject object = new BusinessObject();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, BusinessObject>.Context context)
				throws IOException, InterruptedException {
			String recordValues[] = value.toString().split("::");
			String businessIdStr = recordValues[0];
			String fullAddress = recordValues[1];
			String categories = recordValues[2];
			businessId.set(businessIdStr);
			object.getFullAddress().set(fullAddress);
			object.getCategories().set(categories);
			object.getType().set(TYPES.DETAILS.value);
			context.write(businessId, object);
		}
	}

	public static class AvgTopTenReducer extends Reducer<Text, BusinessObject, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<BusinessObject> values,
				Reducer<Text, BusinessObject, Text, Text>.Context context) throws IOException, InterruptedException {
			String fullAddress = "", categories = "", rating = "";
			for (BusinessObject value : values) {
				if (value.getType().toString().equalsIgnoreCase(TYPES.RATING.value)) {
					rating = value.getRating().toString();
				} else {
					fullAddress = value.getFullAddress().toString();
					categories = value.getCategories().toString();
				}
			}
			if (!rating.equalsIgnoreCase("")) {
				context.write(key, new Text(fullAddress + "\t" + categories + "\t" + rating));
			}
		}
	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: TopTenRated <review-input> <business-input> <output>");
			System.exit(2);
		}
		Path reviewInputPath = new Path(otherArgs[0]);
		Path businessInputPath = new Path(otherArgs[1]);
		Path outputPath = new Path(otherArgs[2]);
		Path tempOutputPath = new Path(otherArgs[2] + "_Temp");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		Job topReviewedJob = Job.getInstance(conf, "TopReviewedJob");
		topReviewedJob.setJarByClass(TopTenRatedFull.class);
		topReviewedJob.setMapperClass(ReviewMapper.class);
		topReviewedJob.setReducerClass(AvgReviewReducer.class);
		topReviewedJob.setMapOutputKeyClass(Text.class);
		topReviewedJob.setMapOutputValueClass(FloatWritable.class);
		topReviewedJob.setOutputKeyClass(Text.class);
		topReviewedJob.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(topReviewedJob, reviewInputPath);
		FileOutputFormat.setOutputPath(topReviewedJob, tempOutputPath);
		boolean firstJobCompleted = topReviewedJob.waitForCompletion(true);

		if (firstJobCompleted) {
			// Reduce side join
			Job topBusinessJob = Job.getInstance(conf, "TopBusinessJob");
			topBusinessJob.setJarByClass(TopTenRatedFull.class);
			MultipleInputs.addInputPath(topBusinessJob, tempOutputPath, TextInputFormat.class, ReviewUtilMapper.class);
			MultipleInputs.addInputPath(topBusinessJob, businessInputPath, TextInputFormat.class, DetailsMapper.class);
			topBusinessJob.setMapOutputKeyClass(Text.class);
			topBusinessJob.setMapOutputValueClass(BusinessObject.class);
			topBusinessJob.setReducerClass(AvgTopTenReducer.class);
			topBusinessJob.setOutputKeyClass(Text.class);
			topBusinessJob.setOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(topBusinessJob, outputPath);
			boolean secondJobCompleted = topBusinessJob.waitForCompletion(true);
			if (secondJobCompleted) {
				if (fs.exists(tempOutputPath)) {
					fs.delete(tempOutputPath, true);
				}
			}
			System.exit(secondJobCompleted ? 0 : 1);
		}
	}
}

class BusinessObject implements Writable {
	private FloatWritable avgRating;
	private Text fullAddress;
	private Text categories;
	private Text type;

	public BusinessObject() {
		avgRating = new FloatWritable(0);
		fullAddress = new Text();
		categories = new Text();
		type = new Text();
	}

	public Text getType() {
		return type;
	}

	public void setType(Text type) {
		this.type = type;
	}

	public FloatWritable getRating() {
		return avgRating;
	}

	public void setRating(FloatWritable rating) {
		this.avgRating = rating;
	}

	public Text getFullAddress() {
		return fullAddress;
	}

	public void setFullAddress(Text fullAddress) {
		this.fullAddress = fullAddress;
	}

	public Text getCategories() {
		return categories;
	}

	public void setCategories(Text categories) {
		this.categories = categories;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		fullAddress.readFields(in);
		categories.readFields(in);
		avgRating.readFields(in);
		type.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		fullAddress.write(out);
		categories.write(out);
		avgRating.write(out);
		type.write(out);
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
