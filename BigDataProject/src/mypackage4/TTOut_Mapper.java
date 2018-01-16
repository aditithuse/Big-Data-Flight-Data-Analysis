package mypackage4;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class TTOut_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
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

		if(delay[20].equals("TaxiOut")||delay[20].equals("Dest"))
		{
			i=1;
		}

		else if (delay[20].equals("NA"))
		{
			i=1;
			context.write(word,two);

		}

		else  {

			word= new Text(delay[17]);
			DoubleWritable one =  new DoubleWritable(Integer.parseInt(delay[20]));	

			context.write(word,one);

		}

	}
}