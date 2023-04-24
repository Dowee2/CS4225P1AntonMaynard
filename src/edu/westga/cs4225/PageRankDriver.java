package edu.westga.cs4225;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
        int maxIterations = 20;
        double convergenceThreshold = 0.0001;
        int iteration = 0;
        Configuration conf = new Configuration();

        Path outputPath = new Path(args[1]);
        Path tempOutputPath = new Path(outputPath, "iteration" + iteration);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(tempOutputPath)) {
            fs.delete(tempOutputPath, true);
        }

        while (iteration < maxIterations) {
            System.out.println("Iteration: " + iteration);
            conf.setInt("iteration", iteration);

            Path currentInputPath;
            if (iteration == 0) {
                currentInputPath = new Path(args[0]);
            } else {
                currentInputPath = new Path(outputPath, "iteration" + (iteration - 1) + "/normalized-ranks");
            }
            System.out.println("Current input path: " + currentInputPath);
            System.out.println("Current output path: " + tempOutputPath);
            tempOutputPath = new Path(outputPath, "iteration" + iteration + "/normalized-ranks");

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

            // Normalize PageRanks after each iteration
            Utils.normalizePageRanks(tempOutputPath, tempOutputPath);

            if (iteration > 0) {
                Path prevInputPath = new Path(outputPath, "iteration" + (iteration - 1) + "/normalized-ranks");
                double difference = Utils.calculatePageRankDifference(prevInputPath, tempOutputPath);
                if (difference < convergenceThreshold) {
                    break;
                }
            }
            iteration++;
        }

        Path finalOutputPath = new Path(outputPath, "final");
        FileSystem.get(conf).rename(new Path(tempOutputPath, "normalized-ranks"), finalOutputPath);
    }

}
