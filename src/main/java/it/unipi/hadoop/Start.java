package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Start {

    // Constants for command-line arguments
    private static final int INPUT_PATH = 0;
    private static final int OUTPUT_PATH = 1;
    private static final int N_REDUCERS = 2;
    private static final int METHOD = 3;

    // Default values for the number of reducers and method
    private static final int DefaultNReducers = 1;
    private static final String DefaultMethod = "InMapper";

    /**
     * Method to calculate the total number of letters from the output files of the first job
     *
     * @param conf the Hadoop configuration
     * @param outputString the output path of the first job
     * @return the total count of letters
     * @throws IOException if an I/O error occurs
     */

    public static long getTotalLetters(Configuration conf, String outputString) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputString);
        FileStatus[] status = fs.listStatus(path);
        long totalLetters = 0;

        // Iterate over each file in the output directory
        for (FileStatus status2 : status) {
            String filename = status2.getPath().getName();
            // Skip the success file
            if (!filename.equals("_SUCCESS")) {
                Path file = new Path(outputString + "/" + filename);
                try (FSDataInputStream inputStream = fs.open(file);
                     BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    // Read and sum up the counts from each line
                    while ((line = bufferedReader.readLine()) != null) {
                        String[] parts = line.split("\\s+");
                        if (parts.length >= 2) {
                            totalLetters += Long.parseLong(parts[1]);
                        }
                    }
                }
            }
        }
        System.out.println("Total Letters: " + totalLetters); // Debugging output
        return totalLetters;
    }

    /**
     * Main method to set up and run the MapReduce jobs
     *
     * @param args the command-line arguments
     */
    
    public static void main(String[] args) {
        try {
            System.out.println("Input Path: " + args[INPUT_PATH]);
            Configuration conf = new Configuration();
            int nReducers = DefaultNReducers;
            String method = DefaultMethod;

            // Parse the number of reducers if provided
            if (args.length > 2 && args[N_REDUCERS] != null) {
                try {
                    nReducers = Integer.parseInt(args[N_REDUCERS]);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid number of reducers provided. Using default value: " + nReducers);
                }
            }

            // Parse the method if provided and validate it
            if (args.length > 3 && args[METHOD] != null) {
                if (args[METHOD].equals("InMapper") || args[METHOD].equals("Combiner")) {
                    method = args[METHOD];
                } else {
                    System.out.println("Invalid method provided. Using default value: " + method);
                }
            }

            // Set up the first job for letter counting
            Job job1 = Job.getInstance(conf, "LetterCount");
            job1.setNumReduceTasks(nReducers);
            System.out.println("Running LetterCount job with " + nReducers + " reducers" + " and method: " + method);

            // Configure the first job based on the selected method
            if (method.equals("InMapper")) {
                job1.setJarByClass(it.unipi.hadoop.InMapper.LetterCount.class);
                job1.setMapperClass(it.unipi.hadoop.InMapper.LetterCount.LetterCounterMapper.class);
                job1.setReducerClass(it.unipi.hadoop.InMapper.LetterCount.LetterCounterReducer.class);
            } else if (method.equals("Combiner")) {
                job1.setJarByClass(it.unipi.hadoop.Combiner.LetterCount.class);
                job1.setMapperClass(it.unipi.hadoop.Combiner.LetterCount.LetterCounterMapper.class);
                job1.setCombinerClass(it.unipi.hadoop.Combiner.LetterCount.LetterCounterReducer.class);
                job1.setReducerClass(it.unipi.hadoop.Combiner.LetterCount.LetterCounterReducer.class);
            }

            // Set the output key and value types for the first job
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(LongWritable.class);
            job1.setInputFormatClass(TextInputFormat.class);
            job1.setOutputFormatClass(TextOutputFormat.class);

            // Add input and output paths for the first job
            FileInputFormat.addInputPath(job1, new Path(args[INPUT_PATH]));

            // Remove the temporary output directory if it exists
            FileSystem fs = FileSystem.get(conf);
            Path tempPath = new Path("temp");
            if (fs.exists(tempPath)) {
                fs.delete(tempPath, true);
            }
            FileOutputFormat.setOutputPath(job1, new Path("temp"));
            job1.waitForCompletion(true);

            // Get the total number of letters from the first job's output
            long totalLetters = getTotalLetters(conf, "temp");

            // Set up the second job for letter frequency calculation
            Job job2 = Job.getInstance(conf, "LetterFrequency");
            job2.setNumReduceTasks(nReducers);

            // Configure the second job based on the selected method
            if (method.equals("InMapper")) {
                job2.setJarByClass(it.unipi.hadoop.InMapper.LetterFrequency.class);
                job2.setMapperClass(it.unipi.hadoop.InMapper.LetterFrequency.FrequencyLetterMapper.class);
                job2.setReducerClass(it.unipi.hadoop.InMapper.LetterFrequency.FrequencyLetterReducer.class);
            } else if (method.equals("Combiner")) {
                job2.setJarByClass(it.unipi.hadoop.Combiner.LetterFrequency.class);
                job2.setMapperClass(it.unipi.hadoop.Combiner.LetterFrequency.FrequencyLetterMapper.class);
                job2.setCombinerClass(it.unipi.hadoop.Combiner.LetterFrequency.FrequencyLetterCombiner.class);
                job2.setReducerClass(it.unipi.hadoop.Combiner.LetterFrequency.FrequencyLetterReducer.class);
            }

            // Set the output key and value types for the second job
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(LongWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            // Add input and output paths for the second job
            FileInputFormat.addInputPath(job2, new Path(args[INPUT_PATH]));
            FileOutputFormat.setOutputPath(job2, new Path(args[OUTPUT_PATH]));

            // Set the total letters as a configuration parameter for the second job
            job2.getConfiguration().setLong("totalLetters", totalLetters);
            System.out.println("Running LetterFrequency job with totalLetters: " + totalLetters);

            // Exit the program based on the success of the second job
            System.exit(job2.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
