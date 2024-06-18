package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import it.unipi.hadoop.Combiner.LetterCount;
import it.unipi.hadoop.Combiner.LetterFrequency;
import it.unipi.hadoop.InMapper.*;

public class Start {
    private static final int INPUT_PATH = 0;
    private static final int OUTPUT_PATH = 1;
    private static final int N_REDUCERS = 2;
    private static final int METHOD = 3;
    private static final int DefaultNReducers = 1;
    private static final String DefaultMethod = "InMapper";

    public static long getTotalLetters(Configuration conf, String outputString) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputString);
        FileStatus[] status = fs.listStatus(path);
        long totalLetters = 0;
        for (FileStatus status2 : status) {
            String filename = status2.getPath().getName();
            if (!filename.equals("_SUCCESS")) {
                Path file = new Path(outputString + "/" + filename);
                FSDataInputStream inputStream = fs.open(file);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    if (parts.length >= 2) {
                        totalLetters += Long.parseLong(parts[1]);
                    }
                }
                bufferedReader.close();
            }
        }
        System.out.println("Total Letters: " + totalLetters); // Debugging output
        return totalLetters;
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            int nReducers = DefaultNReducers;
            String method = DefaultMethod;
            if (args.length > 2 && args[2] != null) {
                try {
                    nReducers = Integer.parseInt(args[2]);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid number of reducers provided. Using default value: " + nReducers);
                }
            }
            
            // Check if args[3] (method) is provided and is either "InMapper" or "Combiner"
            if (args.length > 3 && args[3] != null) {
                if (args[3].equals("InMapper") || args[3].equals("Combiner")) {
                    method = args[3];
                } else {
                    System.out.println("Invalid method provided. Using default value: " + method);
                }
            }
            Job job1 = Job.getInstance(conf, "LetterCountcombiner");
            job1.setNumReduceTasks(nReducers);
            System.out.println("Running LetterCountcombiner job with " + nReducers + " reducers"+ " and method: " + method);
    
            if (method.equals("InMapper")) {
                job1.setJarByClass(it.unipi.hadoop.InMapper.LetterCount.class);
                job1.setMapperClass(it.unipi.hadoop.InMapper.LetterCount.LetterCounterMapper.class);
                job1.setReducerClass(it.unipi.hadoop.InMapper.LetterCount.LetterCounterReducer.class);
            } else if (method.equals("Combiner")) {
                job1.setJarByClass(it.unipi.hadoop.Combiner.LetterCount.class);
                job1.setMapperClass(it.unipi.hadoop.Combiner.LetterCount.LetterCounterMapper.class);
                job1.setReducerClass(it.unipi.hadoop.Combiner.LetterCount.LetterCounterReducer.class);
            }
    
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(LongWritable.class);
            job1.setInputFormatClass(TextInputFormat.class);
            job1.setOutputFormatClass(TextOutputFormat.class);
    
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path("temp"));
            job1.waitForCompletion(true);
    
            long totalLetters = getTotalLetters(conf, "temp");
    
            Job job2 = Job.getInstance(conf, "LetterFrequencycombiner");
            job2.setNumReduceTasks(nReducers);
    
            if (method.equals("InMapper")) {
                job2.setJarByClass(it.unipi.hadoop.InMapper.LetterFrequency.class);
                job2.setMapperClass(it.unipi.hadoop.InMapper.LetterFrequency.FrequencyLetterMapper.class);
                job2.setReducerClass(it.unipi.hadoop.InMapper.LetterFrequency.FrequencyLetterReducer.class);
            } else if (method.equals("combiner")) {
                job2.setJarByClass(it.unipi.hadoop.Combiner.LetterFrequency.class);
                job2.setMapperClass(it.unipi.hadoop.Combiner.LetterFrequency.FrequencyLetterMapper.class);
                job2.setReducerClass(it.unipi.hadoop.Combiner.LetterFrequency.FrequencyLetterReducer.class);
            }
    
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(LongWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job2, new Path(args[0]));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            job2.getConfiguration().setLong("totalLetters", totalLetters);
            System.out.println("Running LetterFrequencycombiner job with totalLetters: " + totalLetters);
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}