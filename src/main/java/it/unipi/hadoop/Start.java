package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import it.unipi.hadoop.LetterCountcombiner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Start {

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
        // read the args and initialize two jobs
        // one for LetterCountcombiner which takes the first input and outputs to a temp file
        // then pass the temp file as input to the second job along with the first input
        // to calculate letter frequencies
        try {
            Configuration conf = new Configuration();
            Integer nReducers= Integer.parseInt(args[2]);
            Job job1 = Job.getInstance(conf, "LetterCountcombiner");
            job1.setNumReduceTasks(nReducers);
            System.out.println("Running LetterCountcombiner job with " + nReducers + " reducers"); // Debugging output
            job1.setJarByClass(LetterCountcombiner.class);
            job1.setMapperClass(LetterCountcombiner.LetterCounterMapper.class);
            job1.setReducerClass(LetterCountcombiner.LetterCounterReducer.class);
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
            job2.setJarByClass(LetterFrequencycombiner.class);
            job2.setMapperClass(LetterFrequencycombiner.FrequencyLetterMapper.class);
            job2.setReducerClass(LetterFrequencycombiner.FrequencyLetterReducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(LongWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job2, new Path(args[0]));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            job2.getConfiguration().setLong("totalLetters", totalLetters);
            System.out.println("Running LetterFrequencycombiner job with totalLetters: " + totalLetters); // Debugging output
            System.exit(job2.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
