package MapReduceApp;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class MapReduce {

	public static void main(String[] args) throws Exception {
		
		// missing needed arguments
		if(args.length<4){
			System.out.println("missing arguments");
			return;
		}
		
		// part 1: takes in list of files
		File folder = null;
		
		if(args[0].equalsIgnoreCase("-files")){
			folder = new File(args[1]);
			if(!folder.exists()){
				System.out.println("file dosent exist");
				return;
			}
		}
		
		// use function to get all files
		Map<String, String> input = new HashMap<String, String>();
		GetFiles(folder,input);

		// APPROACH #1: Brute force
		{
			Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

			Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
			while (inputIter.hasNext()) {
				Map.Entry<String, String> entry = inputIter.next();
				String file = entry.getKey();
				String contents = entry.getValue();

				String[] words = contents.trim().split("\\s+");

				for (String word : words) {

					Map<String, Integer> files = output.get(word);
					if (files == null) {
						files = new HashMap<String, Integer>();
						output.put(word, files);
					}

					Integer occurrences = files.remove(file);
					if (occurrences == null) {
						files.put(file, 1);
					} else {
						files.put(file, occurrences.intValue() + 1);
					}
				}
			}
			System.out.println("Approach 1");
			// show me:
			System.out.println(output);
		}

		// APPROACH #2: MapReduce
		{
			Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>(); // map interface implemented as HashMap..??

			// MAP:

			List<MappedItem> mappedItems = new LinkedList<MappedItem>();

			Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
			while (inputIter.hasNext()) {
				Map.Entry<String, String> entry = inputIter.next();
				String file = entry.getKey();
				String contents = entry.getValue();

				map(file, contents, mappedItems);
			}

			// GROUP:

			Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

			Iterator<MappedItem> mappedIter = mappedItems.iterator();
			while (mappedIter.hasNext()) {
				MappedItem item = mappedIter.next();
				String word = item.getWord();
				String file = item.getFile();
				List<String> list = groupedItems.get(word);
				if (list == null) {
					list = new LinkedList<String>();
					groupedItems.put(word, list);
				}
				list.add(file);
			}

			// REDUCE:

			Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
			while (groupedIter.hasNext()) {
				Map.Entry<String, List<String>> entry = groupedIter.next();
				String word = entry.getKey();
				List<String> list = entry.getValue();

				reduce(word, list, output);
			}
			System.out.println("\nApproach 2");
			System.out.println(output);
		}

		// APPROACH #3: Distributed MapReduce
		{
			final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

			// MAP:

			final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

			final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
				@Override
				public synchronized void mapDone(String file, List<MappedItem> results) {
					mappedItems.addAll(results);
				}
			};

			List<Thread> mapCluster = new ArrayList<Thread>(input.size());

			Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
			while (inputIter.hasNext()) {
				Map.Entry<String, String> entry = inputIter.next();
				final String file = entry.getKey();
				final String contents = entry.getValue();

				Thread t = new Thread(new Runnable() {
					@Override
					public void run() {
						map(file, contents, mapCallback);
					}
				});
				mapCluster.add(t);
				t.start();
			}

			// wait for mapping phase to be over:
			for (Thread t : mapCluster) {
				try {
					t.join();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

			// GROUP:

			Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

			Iterator<MappedItem> mappedIter = mappedItems.iterator();
			while (mappedIter.hasNext()) {
				MappedItem item = mappedIter.next();
				String word = item.getWord();
				String file = item.getFile();
				List<String> list = groupedItems.get(word);
				if (list == null) {
					list = new LinkedList<String>();
					groupedItems.put(word, list);
				}
				list.add(file);
			}

			// REDUCE:

			final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
				@Override
				public synchronized void reduceDone(String k, Map<String, Integer> v) {
					output.put(k, v);
				}
			};

			List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

			Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
			while (groupedIter.hasNext()) {
				Map.Entry<String, List<String>> entry = groupedIter.next();
				final String word = entry.getKey();
				final List<String> list = entry.getValue();

				Thread t = new Thread(new Runnable() {
					@Override
					public void run() {
						reduce(word, list, reduceCallback);
					}
				});
				reduceCluster.add(t);
				t.start();
			}

			// wait for reducing phase to be over:
			for (Thread t : reduceCluster) {
				try {
					t.join();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			System.out.println("\nApproach 3");
			System.out.println(output);
		}
	}

	public static void map(String file, String contents, List<MappedItem> mappedItems) {
		String[] words = contents.trim().split("\\s+");
		for (String word : words) {
			mappedItems.add(new MappedItem(word, file));
		}
	}

	public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
		Map<String, Integer> reducedList = new HashMap<String, Integer>();
		for (String file : list) {
			Integer occurrences = reducedList.get(file);
			if (occurrences == null) {
				reducedList.put(file, 1);
			} else {
				reducedList.put(file, occurrences.intValue() + 1);
			}
		}
		output.put(word, reducedList);
	}

	public static interface MapCallback<E, V> {

		public void mapDone(E key, List<V> values);
	}

	public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
		String[] words = contents.trim().split("\\s+");
		List<MappedItem> results = new ArrayList<MappedItem>(words.length);
		for (String word : words) {
			results.add(new MappedItem(word.substring(0, 1), file));
		}
		callback.mapDone(file, results);
	}

	public static interface ReduceCallback<E, K, V> {

		public void reduceDone(E e, Map<K, V> results);
	}

	public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

		Map<String, Integer> reducedList = new HashMap<String, Integer>();
		for (String file : list) {
			Integer occurrences = reducedList.get(file);
			if (occurrences == null) {
				reducedList.put(file, 1);
			} else {
				reducedList.put(file, occurrences.intValue() + 1);
			}
		}
		callback.reduceDone(word, reducedList);
	}

	private static class MappedItem {

		private final String word;
		private final String file;

		public MappedItem(String word, String file) {
			this.word = word;
			this.file = file;
		}

		public String getWord() {
			return word;
		}

		public String getFile() {
			return file;
		}

		@Override
		public String toString() {
			return "[\"" + word + "\",\"" + file + "\"]";
		}
	}
	
	//function for part 1
	private static void GetFiles(File Dir, Map<String,String> input) throws FileNotFoundException{
		//gets files in directory 
		File[] fileList =Dir.listFiles(); 
		
		// goes through each file
		for(File file : fileList){
			
			// if its a sub directory 
			if(!file.isFile()){
				// goes through its files
				GetFiles(file,input);
				continue;
			}
			
			//read contents of file
			Scanner read = new Scanner(file);
			String contents = read.useDelimiter("\\Z").next();
			
			//add file and contents 
			input.put(file.getName(),contents);
		}
	}
}