package BACKUP;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

class Paira implements WritableComparable<Pair> {
 int iSum;
 int iCount;
 
 Pair() {};
 Pair(int sum, int count) {
  iSum = sum;
  iCount = count;
 }
 
 public int getiSum() {
  return iSum;
 }
 
 public int getiCount() {
  return iCount;
 }
 
 @Override
 public void readFields(DataInput dataInput) throws IOException {
  iSum = dataInput.readInt();
  iCount = dataInput.readInt();
 }
 
 @Override
 public void write(DataOutput arg0) throws IOException {
  // TODO Auto-generated method stub
  arg0.writeInt(iSum);
  arg0.writeInt(iCount);
 }
 @Override
 public int compareTo(Pair o) {
  // TODO Auto-generated method stub
  return 0;
 }
}
public class AvgTempCalcWithCombiner extends Configured implements Tool {
 
 public static void main(String[] args) throws Exception {
  System.out.println(Arrays.toString(args));
  Configuration conf = new Configuration();
    
  int res = ToolRunner.run(conf, new AvgTempCalcWithCombiner(), args);
  
  System.exit(res);
 }
 
  
 
 @Override
 public int run(String[] args) throws Exception {
  
  Job job = new Job(getConf(), "AvgTempCalcWithCombiner");
  job.setJarByClass(AvgTempCalcWithCombiner.class);
  
  job.setMapOutputKeyClass(Text.class);
  job.setMapOutputValueClass(Pair.class);
  
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(DoubleWritable.class);
  job.setMapperClass(Map.class);
  job.setReducerClass(Reduce.class);
  job.setInputFormatClass(TextInputFormat.class);
  job.setOutputFormatClass(TextOutputFormat.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  job.waitForCompletion(true);
  return 0;
 }
 
 
 public static class Map extends Mapper<LongWritable, Text, Text, Pair> {
  
  private HashMap<String, Pair> sumCnt = new HashMap<>();
  private int sumTemp = 0;
  private int cnt = 0;
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
//   pair = new Pair();
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
   
   String strYear = value.toString().substring(15, 19);
   int tempLastIndex = value.toString().lastIndexOf("+") - 1;
   int tempBeginIndex = tempLastIndex - 5;
   int temperture = Integer.parseInt(value.toString().substring(tempBeginIndex, tempLastIndex));
   
   if(sumCnt.containsKey(strYear)) {
    sumTemp = sumCnt.get(strYear).iSum + temperture;
    cnt += 1;
    sumCnt.put(strYear, new Pair(sumTemp, cnt));
   }
   else {
    cnt = 1;
    sumCnt.put(strYear, new Pair(temperture, cnt));
   }
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
   
   Iterator<Entry<String, Pair>> iterator = sumCnt.entrySet().iterator();
   while(iterator.hasNext()) {
    Entry<String, Pair> entry = iterator.next();
    String year = entry.getKey();
    int totalSum = entry.getValue().iSum;
    int totalCnt = entry.getValue().iCount;
    context.write(new Text(year), new Pair(totalSum, totalCnt));
   }
  }
 }
 public static class Reduce extends Reducer<Text, Pair, Text, DoubleWritable> {
  @Override
  public void reduce(Text key, Iterable<Pair> values, Context context)
    throws IOException, InterruptedException {
   
   for (Pair val : values) {
    double sum = (double)val.getiSum();
    int cnt = val.getiCount();
    double avg = sum / cnt;
    context.write(key, new DoubleWritable(avg));
   }
  }
 }
}