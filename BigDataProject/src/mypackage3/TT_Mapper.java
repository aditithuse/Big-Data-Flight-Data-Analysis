package mypackage3;

import java.io.IOException;
//import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public  class TT_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
	//private final static DoubleWritable one ;
	private Text word = new Text();
	private final static DoubleWritable two = new DoubleWritable(0);
	//String  word;
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		int i=0;
		String line = value.toString();
		String [] delay=line.split(",");

		if(delay[19].equals("TaxiIn")||delay[19].equals("Dest"))
		{
			i=1;
		}
		else if (delay[19].equals("NA"))
		{
			i=1;
			context.write(word,two);
		}

		else  {

			word= new Text(delay[17]);
			DoubleWritable one =  new DoubleWritable(Integer.parseInt(delay[19]));
			context.write(word,one);
		}

	}
}