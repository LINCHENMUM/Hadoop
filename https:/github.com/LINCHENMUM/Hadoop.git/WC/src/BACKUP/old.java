package BACKUP;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AvgTemperatureInMapperCombiningOLD extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Configuration conf = new Configuration();
				
		int res = ToolRunner.run(conf, new AvgTemperatureInMapperCombining(), args);
		
		System.exit(res);
	}
	
	public static class UPair{
		private double sumTemp;
		private int yearCount;
		
		public UPair(double s,int c){
			this.sumTemp=s;
			this.yearCount=c;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = new Job(getConf(), "AvgTemperatureInMapperCombining");
		job.setJarByClass(AvgTemperatureInMapperCombining.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		//job.setOutputValueClass(UPair.class);

		job.setMapperClass(MapAvgTemp.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static class MapAvgTemp extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		private Text word = new Text();
		private final DoubleWritable totalCountDouTemp = new DoubleWritable(); 
		private Map<String, Double> avgTemp;
		private UPair yearCountPair=new UPair(0.0, 0);
		 
		@Override  
		protected void setup(Context context) throws IOException, InterruptedException {  
		      super.setup(context);  
		  
		      avgTemp = new HashMap<String, Double>();    
		    }  
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String year;
			String line;
			Double temp;
			int yearC=0;
			Double tempS=0.0;
			Double avgT=0.0;

						
			line = value.toString();
			year = line.substring(15, 19);
			System.out.println("year" + year);
			temp = Double.parseDouble(line.substring(line.length() - 18,
					line.length() - 13));
			System.out.println("Temp" + temp);

			if (avgTemp.containsKey(year)) {
				// System.out.println("avgTemp.get(year)"+avgTemp.get(year));
				yearC=yearC+1;
				tempS=avgTemp.get(year) + temp;
				System.out.println("yearC-------"+yearC);
				System.out.println("avgTemp.get(year)------"+avgTemp.get(year));
				avgT=tempS/yearC;
				System.out.println("AVG------"+avgT);
				avgTemp.put(year, avgT);
			} else {
				yearC+=1;
				tempS=temp;
				avgT=tempS/yearC;
				avgTemp.put(year, avgT);
			}
	            
			 
		}
		@Override  
		protected void cleanup(Context context) throws IOException, InterruptedException {  
		  
		        // Emit each word as well as its count  
			 for (Map.Entry<String, Double> entry : avgTemp.entrySet()) {  
					word.set(entry.getKey());  
					System.out.println("entry.getKey()"+entry.getKey());
					totalCountDouTemp.set(entry.getValue());  
					System.out.println("totalCountDouTemp"+totalCountDouTemp);
					System.out.println("entry.getValue()"+entry.getValue());
		            context.write(word, totalCountDouTemp);  
		        }  
		        super.cleanup(context);  
		    }  	
	}

	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			double avg=0;
			int count=0;
			for (DoubleWritable val : values) {
				sum += val.get();
				System.out.println("sum"+sum);
				count+=1;
				System.out.println("count"+count);
			}
			
			avg=sum/count;
			context.write(key, new DoubleWritable(avg));
		}
	}
}

