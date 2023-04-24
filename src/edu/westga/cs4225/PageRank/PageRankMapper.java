package edu.westga.cs4225.PageRank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.westga.cs4225.Utils.DatasetParser;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, PageRankData> {
    private static final double DAMPING_FACTOR = 0.85;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        DatasetParser parser = new DatasetParser();
        List<PageRankData> pageRankDataList = parser.parse(value.toString());

        // Loop through each PageRankData object
        for (PageRankData pageRankData : pageRankDataList) {
            String pageTitle = pageRankData.pageTitle;
            ArrayList<String> inLinks = new ArrayList<>();
            ArrayList<String> outLinks = new ArrayList<>();

            // Loop through each PageRankData object to find pages that link to the current page
            for (PageRankData otherPageRankData : pageRankDataList) {
                if (otherPageRankData != pageRankData) {
                    if (otherPageRankData.outLinks != null && otherPageRankData.outLinks.contains(pageTitle)) {
                        inLinks.add(otherPageRankData.pageTitle);
                    }
                    if (pageRankData.outLinks != null && pageRankData.outLinks.contains(otherPageRankData.pageTitle)) {
                        outLinks.add(otherPageRankData.pageTitle);
                    }
                }
            }

            // Calculate the new page rank based on the in-links and previous page rank
            double newPageRank = 0.0;
            for (String inLink : inLinks) {
                double outLinkCount = outLinks.size();
                newPageRank += DAMPING_FACTOR * (pageRankData.pageRank / outLinkCount);
            }

            newPageRank += (1 - DAMPING_FACTOR);

            // Update the PageRankData object with the new page rank, in-links, and out-links
            pageRankData.inLinks = inLinks;
            pageRankData.outLinks = outLinks;
            pageRankData.newPageRank = newPageRank;

            // Output the PageRankData object with the page title as the key
            context.write(new Text(pageTitle), pageRankData);
        }
    }
}

