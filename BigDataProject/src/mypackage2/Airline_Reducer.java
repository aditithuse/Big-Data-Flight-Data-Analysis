package mypackage2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Airline_Reducer {
	java.util.Map<String, Double> list = new HashMap();
	int total = 0;

	public Airline_Reducer() {
	}

	public void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		int fcount = 0;
		java.util.Map<String, Double> jmap = new HashMap();
		Iterator var6 = values.iterator();

		while(var6.hasNext()) {
			DoubleWritable value = (DoubleWritable)var6.next();
			String airline = key.toString();
			++fcount;
			++this.total;
			if (jmap.containsKey(airline)) {
				jmap.put(airline, ((Double)jmap.get(airline)).doubleValue() + value.get());
			} else {
				jmap.put(airline, value.get());
			}
		}

		var6 = jmap.keySet().iterator();

		while(var6.hasNext()) {
			String word = (String)var6.next();
			this.list.put(word, jmap.get(word));
		}

	}

	public void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		new ArrayList();
		List<Entry<String, Double>> fin = getThreeBest(this.list, this.total);
		context.write(new Text("Three Best Airlines:"), new DoubleWritable());
		Iterator var3 = fin.iterator();

		Entry entry;
		while(var3.hasNext()) {
			entry = (Entry)var3.next();
			context.write(new Text((String)entry.getKey()), new DoubleWritable(((Double)entry.getValue()).doubleValue()));
		}

		fin.clear();
		fin = getThreeWorst(this.list, this.total);
		context.write(new Text("Three Worst Airlines:"), new DoubleWritable());
		var3 = fin.iterator();

		while(var3.hasNext()) {
			entry = (Entry)var3.next();
			context.write(new Text((String)entry.getKey()), new DoubleWritable(((Double)entry.getValue()).doubleValue()));
		}
	}

	public static List<Entry<String, Double>> getThreeWorst(java.util.Map<String, Double> unsortMap, int tot) {
		List<Entry<String, Double>> sorted = new ArrayList(unsortMap.entrySet());
		List<Entry<String, Double>> result = new ArrayList();
		Iterator var5 = sorted.iterator();

		Entry entry;
		while(var5.hasNext()) {
			entry = (Entry)var5.next();
			entry.setValue(((Double)entry.getValue()).doubleValue() / (double)tot);
		}

		Collections.sort(sorted, new Comparator<Entry<String, Double>>() {
			public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
				return ((Double)o1.getValue()).compareTo((Double)o2.getValue());
			}
		});
		Collections.reverse(sorted);
		int count = 1;
		var5 = sorted.iterator();

		while(var5.hasNext()) {
			entry = (Entry)var5.next();
			if (count <= 3) {
				result.add(entry);
				++count;
			}
		}

		return result;
	}

	public static List<Entry<String, Double>> getThreeBest(java.util.Map<String, Double> unsortMap, int tot) {
		List<Entry<String, Double>> sorted = new ArrayList(unsortMap.entrySet());
		List<Entry<String, Double>> result = new ArrayList();
		Iterator var5 = sorted.iterator();

		Entry entry;
		while(var5.hasNext()) {
			entry = (Entry)var5.next();
			entry.setValue(((Double)entry.getValue()).doubleValue() / (double)tot);
		}

		Collections.sort(sorted, new Comparator<Entry<String, Double>>() {
			public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
				return ((Double)o1.getValue()).compareTo((Double)o2.getValue());
			}
		});
		int count = 1;
		var5 = sorted.iterator();

		while(var5.hasNext()) {
			entry = (Entry)var5.next();
			if (count <= 3) {
				result.add(entry);
				++count;
			}
		}

		return result;
	}
}
