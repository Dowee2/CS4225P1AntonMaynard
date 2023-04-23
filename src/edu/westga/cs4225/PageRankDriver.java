package edu.westga.cs4225;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.westga.cs4225.PageRank.PageRankData;
import edu.westga.cs4225.PageRank.PageRankMapper;
import edu.westga.cs4225.PageRank.PageRankReducer;
import edu.westga.cs4225.Utils.Utils;

public class PageRankDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PageRankDriver <input path> <output path>");
            System.exit(-1);
        }
    
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path tempOutputPath = new Path(outputPath, "temp");
    
        int maxIterations = 20;
        double convergenceThreshold = 0.0001;
        int iteration = 0;
    
        while (iteration < maxIterations) {
            System.out.println("Iteration: " + iteration);
            conf.setInt("iteration", iteration);
    
            Path currentInputPath;
            if (iteration == 0) {
                currentInputPath = new Path(args[0]);
            } else {
                currentInputPath = new Path(outputPath, "iteration" + (iteration - 1));
            }
            Path currentOutputPath = new Path(outputPath, "iteration" + iteration);
            tempOutputPath = new Path(outputPath, "iteration" + iteration); // Update tempOutputPath for each iteration
            System.out.println("Current input path: " + currentInputPath);
            System.out.println("Current temp output path: " + tempOutputPath);
    
            Job job = Job.getInstance(conf, "PageRank");
            job.setJarByClass(PageRankDriver.class);
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);
    
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(PageRankData.class);
    
            job.setInputFormatClass(TextInputFormat.class);
    
            FileInputFormat.addInputPath(job, currentInputPath);
            FileOutputFormat.setOutputPath(job, tempOutputPath);
    
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
    
            if (iteration > 0) {
                double difference = Utils.calculatePageRankDifference(currentInputPath, tempOutputPath);
                if (difference < convergenceThreshold) {
                    break;
                }
            }
            iteration++;
        }
    
        // Normalize PageRank values so the maximum PageRank is 1
        //double maxPageRank = Utils.findMaxPageRank(tempOutputPath);
        Path normalizedOutputPath = new Path(outputPath, "normalized");
        Utils.normalizePageRanks(tempOutputPath, normalizedOutputPath);
    }        
}
