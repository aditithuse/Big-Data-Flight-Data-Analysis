package mypackage;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map.Entry;

public class Can_Reducer extends
Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	java.util.Map<String, Double> reason = new HashMap<String, Double>();

	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		// Variable to hold the count of word
		java.util.Map<String, Double> jmap = new HashMap<String, Double>();

		for (DoubleWritable value : values) {
			String rsn = key.toString();

			if (jmap.containsKey(rsn)) 
				jmap.put(rsn, jmap.get(rsn) + value.get());
			
			else 
				jmap.put(rsn, value.get());
			
		}

		for (String word : jmap.keySet()) {
			reason.put(word, jmap.get(word));
		}
	}

	public void cleanup(Context context) throws IOException,
	InterruptedException {
		// First perform the task on Reason List
		List<Entry<String, Double>> fin = new ArrayList<Entry<String, Double>>();
		fin = getReason(reason);

		Text t =new Text("Most common reason for cancellation");
		context.write(t,
				new DoubleWritable());
		for (java.util.Map.Entry<String, Double> entry : fin) {
			context.write(new Text(entry.getKey()),new DoubleWritable(entry.getValue()));
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

		// Reverse the list
		Collections.reverse(sorted);

		for (java.util.Map.Entry<String, Double> entry : sorted) {
			result.add(entry);
			break;
		}
		return result;
	}

	// @Override

}
