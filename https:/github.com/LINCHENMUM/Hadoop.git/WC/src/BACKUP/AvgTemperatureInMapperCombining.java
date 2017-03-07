package BACKUP;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

class Pair implements WritableComparable<Pair> {
	int tempSum;
	int tempYear;

	Pair() {
	};

	public Pair(int s, int y) {
		this.tempSum = s;
		this.tempYear = y;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		tempSum = arg0.readInt();
		tempYear = arg0.readInt();

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeInt(tempSum);
		arg0.writeInt(tempYear);

	}

	public int getTempSum() {
		return tempSum;
	}

	public void setTempSum(int tempSum) {
		this.tempSum = tempSum;
	}

	public int getTempYear() {
		return tempYear;
	}

	public void setTempYear(int tempYear) {
		this.tempYear = tempYear;
	}

	@Override
	public int compareTo(Pair o) {
		// TODO Auto-generated method stub
		return 0;
	}
}

public class AvgTemperatureInMapperCombining extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AvgTemperatureInMapperCombining(),
				args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "AvgTemperatureInMapperCombining");
		job.setJarByClass(AvgTemperatureInMapperCombining.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pair.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(MapAvgTemp.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static class MapAvgTemp extends
			Mapper<LongWritable, Text, Text, Pair> {

		private Text word = new Text();
		private int sumTemperature = 0;
		private int sumCount = 0;
		private Map<String, Pair> avgTemp;

		int temperature;
		int yearC = 0;
		int temperatureS = 0;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			avgTemp = new HashMap<String, Pair>();
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			String year = line.substring(15, 19);
			temperature = Integer.parseInt(line.substring(line.length() - 18,
					line.length() - 13));

			if (avgTemp.containsKey(year)) {
				yearC += 1;
				temperatureS = temperature + avgTemp.get(year).tempSum;
				avgTemp.put(year, new Pair(temperatureS, yearC));
			} else {
				yearC = 1;
				temperatureS = temperature;
				avgTemp.put(year, new Pair(temperatureS, yearC));
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			// Emit each word as well as its count
			for (Map.Entry<String, Pair> entry : avgTemp.entrySet()) {
				word.set(entry.getKey());
				sumTemperature = entry.getValue().tempSum;
				sumCount = entry.getValue().tempYear;
				context.write(word, new Pair(sumTemperature, sumCount));
			}
			super.cleanup(context);
		}
	}

	public static class Reduce extends
			Reducer<Text, Pair, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<Pair> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			double avg = 0;
			int count = 0;
			for (Pair val : values) {
				sum = val.getTempSum();
				count = val.getTempYear();
				// System.out.println("count" + count);
				avg = sum / count;
				context.write(key, new DoubleWritable(avg));
			}
		}
	}
}

// 1949 94.5
// 1950 3.6666666666666665

