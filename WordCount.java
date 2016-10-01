import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*

Team Members :

Pradyumna Reddy Kortha (50167960,pkortha)
Bhargav Repuri (50133959,brepuri)

*/



public class WordCount {
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

        public static boolean isInteger(String s) {
		    try { 
		        Integer.parseInt(s); 
		    } catch(NumberFormatException e) { 
		        return false; 
		    } catch(NullPointerException e) {
		        return false;
		    }
		    return true;
		}        

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String itr = value.toString();
			String[] classesdata = itr.split(",");

			if(!classesdata[1].isEmpty() && !classesdata[2].isEmpty() && !classesdata[1].equalsIgnoreCase("Unknown") && !classesdata[2].equalsIgnoreCase("Unknown") && !classesdata[2].substring(0,3).equalsIgnoreCase("Arr") && isInteger(classesdata[7]) && Integer.valueOf(classesdata[7])>=0 && classesdata.length==9)
			{
				String Hall = classesdata[2].split(" ")[0];
				String Sem = classesdata[1];
				String o = Hall+"_"+Sem;
				Integer student_count = Integer.valueOf(classesdata[7]);
				IntWritable new_count = new IntWritable(student_count);
					word.set(o);
					context.write(word, new_count);
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
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
