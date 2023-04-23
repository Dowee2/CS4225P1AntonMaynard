package edu.westga.cs4225.PageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducer extends Reducer<Text, PageRankData, Text, PageRankData> {
    private double dampingFactor = 0.85;


    public void reduce(Text key, Iterable<PageRankData> values, Context context) throws IOException, InterruptedException {
        double sumPageRanks = 0.0;
        PageRankData finalPageRankData = null;
        //finalPageRankData.pageTitle = key.toString(); // Initialize with the current key
        // finalPageRankData.outLinks = new String[]{};

        for (PageRankData value : values) {
            sumPageRanks += value.pageRank;
            if (finalPageRankData == null || finalPageRankData.outLinks.length == 0) {
                finalPageRankData = new PageRankData(value.pageTitle, value.pageRank, value.outLinks);
            }
        }

        finalPageRankData.pageRank = (1 - dampingFactor) + dampingFactor * sumPageRanks;

        System.out.println("Reducer output key: " + key);
        System.out.println("Reducer output value: " + finalPageRankData);
        context.write(key, finalPageRankData);
    }
}

