package mypackage2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AirlineDelay_Runner {

		public static void main(String[] args) throws Exception {
			Job job = new Job();
			job.setJarByClass(AirlineDelay.class);
			job.setMapperClass(AirlineDelay.Map.class);
			job.setReducerClass(AirlineDelay.Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}