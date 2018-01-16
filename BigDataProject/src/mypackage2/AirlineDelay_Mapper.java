package mypackage2;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirlineDelay_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		public AirlineDelay_Mapper() {
		}

		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
			if (key.get() > 0L) {
				String line = value.toString();
				String[] words = line.split(",");
				int arrDelay;
				if (words[14].equalsIgnoreCase("NA")) {
					arrDelay = 0;
				} else {
					arrDelay = Integer.parseInt(words[14]);
				}

				int depDelay;
				if (words[15].equalsIgnoreCase("NA")) {
					depDelay = 0;
				} else {
					depDelay = Integer.parseInt(words[15]);
				}

				double totalDelay = (double)(arrDelay + depDelay);
				if (totalDelay > 5.0D) {
					context.write(new Text(words[8]), new DoubleWritable(1.0D));
				}
			}

		}
	}