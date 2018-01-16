package mypackage3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TT_Reducer extends
Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	java.util.Map<String, Double> reason = new HashMap<String, Double>();
	double sum=0;
	double mean=0;
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		// Variable to hold the count of word

		java.util.Map<String, Double> jmap = new HashMap<String, Double>();

		for (DoubleWritable value : values) {
			String rsn = key.toString();

			if(value.get()==2){
				sum++;	
			}
			else{
				sum++;
				jmap.put(rsn, (mean+value.get()));
			}
		}

		for (String word : jmap.keySet()) {
			reason.put(word, (jmap.get(word)));
		}
	}

	public void cleanup(Context context) throws IOException,
	InterruptedException {
		// First perform the task on Reason List
		List<Entry<String, Double>> fin = new ArrayList<Entry<String, Double>>();
		fin = getReason(reason);
		int j=0;
		Text t= new Text("Shortest taxi in time");
		Text t1= new Text("Longest taxi in time");
		for (java.util.Map.Entry<String, Double> entry : fin) {
			if(j==0)
			{
				context.write(t,new DoubleWritable(0.00));
			}
			if(j==3)
			{
				context.write(t1,new DoubleWritable(0.00));
			}
			context.write(new Text(entry.getKey()),
					new DoubleWritable(entry.getValue()));
			j++;
		}
	}

	public static List<Entry<String, Double>> getReason(
			java.util.Map<String, Double> unsortMap) {
		// Convert Map to List
		List<Entry<String, Double>> sorted = new ArrayList<Entry<String, Double>>(
				unsortMap.entrySet());
		List<Entry<String, Double>> result = new ArrayList<Entry<String, Double>>();

		// Sort list with comparator
		Collections.sort(sorted,
				new Comparator<java.util.Map.Entry<String, Double>>() {
			public int compare(java.util.Map.Entry<String, Double> o1,
					java.util.Map.Entry<String, Double> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});
		int j=0;
		for (java.util.Map.Entry<String, Double> entry : sorted) {
			j++;
			if(j>3)
				break;
			else
				result.add(entry);
		}
		Collections.reverse(sorted);
		int i=0;
		for (java.util.Map.Entry<String, Double> entry : sorted) {
			i++;
			if(i>3)
				break;
			else
				result.add(entry);
			//break;
		}
		return result;
	}

	// @Override

}
