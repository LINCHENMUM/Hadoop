import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

class textPair implements WritableComparable<Pair> {
	String termWord;
	String docID;

	textPair() {
	};

	public textPair(String t, String d) {
		this.termWord = t;
		this.docID = d;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		termWord = arg0.readLine();
		docID = arg0.readLine();

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeChars(termWord);
		arg0.writeChars(docID);

	}

	public String getTermWord() {
		return termWord;
	}

	public void setTermWord(String termWord) {
		this.termWord = termWord;
	}

	public String getDocID() {
		return docID;
	}

	public void setDocID(String docID) {
		this.docID = docID;
	}

	@Override
	public int compareTo(Pair o) {
		// TODO Auto-generated method stub
		return 0;
	}
}

class DocsWritable implements Writable {

	private HashMap<String, Integer> docMap = new HashMap<String, Integer>();

	public DocsWritable() {
	}

	public DocsWritable(HashMap<String, Integer> docMap) {
		this.docMap = docMap;
	}

	private Integer getCount(String key) {
		return docMap.get(key);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		Iterator<String> iterator = docMap.keySet().iterator();
		Text key = new Text();
		while (iterator.hasNext()) {
			String keyNext = iterator.next();
			key = new Text(keyNext);
			key.readFields(arg0);
			new IntWritable(getCount(keyNext)).readFields(arg0);
		}
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		Iterator<String> iterator = docMap.keySet().iterator();
		// Text keyNext=new Text();
		// IntWritable count=new IntWritable();

		while (iterator.hasNext()) {
			String keyOne = iterator.next();
			new Text(keyOne).write(arg0);
			new IntWritable(getCount(keyOne)).write(arg0);
		}
	}

	@Override
	public String toString() {
		String output = "";
		for (String key : docMap.keySet()) {
			output += (key + "-->" + getCount(key).toString() + "");
		}
		return output;
	}
}

public class RevisedDocuments extends Configured implements Tool {

	public static Map<textPair, Integer> textCal;

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new RevisedDocuments(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "RevisedDocuments");
		job.setJarByClass(RevisedDocuments.class);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(IntWritable.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DocsWritable.class);

		job.setMapperClass(IndexMap.class);
		job.setReducerClass(IndexReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static class IndexMap extends Mapper<Text, Text, textPair, Integer> {
	//public static class IndexMap extends MapReduceBase implements Mapper<Text,Text,textPair, Integer>{

		private textPair textpair;
		private int termCount = 0;
		private Integer sumCount;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			textCal = new HashMap<textPair, Integer>();
		}

		// private final static IntWritable ONE = new IntWritable(1);

		// @Override
		public void map(Text key, Text value,
				OutputCollector<textPair, IntWritable> output, Reporter reporter)
				throws IOException, InterruptedException {

			System.out.println("Start mapping........................");
			FileSplit filesplit = (FileSplit) reporter.getInputSplit();
			String file = filesplit.getPath().getName();
			System.out.println("File name=" + file);
			// String line = value.toString();

			for (String token : value.toString().split("\\s+")) {
				// word.set(token);
				// context.write(word, ONE);

				textpair = new textPair(file, token);

				if (textCal.containsKey(textpair)) {
					termCount += 1;
					textCal.put(textpair, termCount);
				} else {
					termCount = 1;
					textCal.put(textpair, termCount);
				}

			}

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// Emit each word as well as its count

			for (Map.Entry<textPair, Integer> entry : textCal.entrySet()) {
				sumCount = sumCount + entry.getValue();
				context.write(entry.getKey(), sumCount);
			}

			super.cleanup(context);
		}
	}

	public static class IndexReduce extends
			Reducer<textPair, Text, Text, DocsWritable> {
		// @Override
		public void reduce(textPair key, Map.Entry<textPair, Integer> value,
				Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> outMap = new HashMap<String, Integer>();

			Text termName = new Text();
			String docID = "";

			for (Map.Entry<textPair, Integer> entry : textCal.entrySet()) {
				termName.set(entry.getKey().termWord);
				docID = entry.getKey().docID;
				outMap.put(docID, entry.getValue());
				context.write(termName, new DocsWritable(outMap));
			}
		}
	}
}
