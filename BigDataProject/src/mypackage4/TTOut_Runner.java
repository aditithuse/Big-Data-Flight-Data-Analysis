package mypackage4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
public class TTOut_Runner {
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Job job = new Job();
		job.setJarByClass(TTOut_Runner.class);
		job.setMapperClass(TTOut_Mapper.class);
		job.setReducerClass(TTOut_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}