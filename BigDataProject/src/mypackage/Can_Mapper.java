package mypackage;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public  class Can_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
	private final static DoubleWritable one = new DoubleWritable(1);
	private Text word = new Text();
	//String  word;
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String [] cancel_reason=line.split(",");


		if(cancel_reason[21].equalsIgnoreCase("1")){

			if(cancel_reason[22].equalsIgnoreCase("A")){
				word=new Text("carrier");
			}else if(cancel_reason[22].equalsIgnoreCase("B")){
				word=new Text("weather");
			}else if(cancel_reason[22].equalsIgnoreCase("C")){
				word=new Text("NAS");
			}else{
				word=new Text("security");
			}

			context.write(word,one);
		}

	}
}



