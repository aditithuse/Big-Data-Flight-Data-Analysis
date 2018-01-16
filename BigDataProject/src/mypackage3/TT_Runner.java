package mypackage3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;



public class TT_Runner {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{

		Job job = new Job();
		job.setJarByClass(TT_Runner.class);
		job.setMapperClass(TT_Mapper.class);
		job.setReducerClass(TT_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0])); //input

		FileOutputFormat.setOutputPath(job, new Path(args[1])); //output

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}