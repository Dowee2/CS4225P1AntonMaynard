package edu.westga.cs4225.PageRank;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, PageRankData, Text, PageRankData> {

    private static final double DAMPING_FACTOR = 0.85;

    @Override
    protected void reduce(Text key, Iterable<PageRankData> values, Context context)
            throws IOException, InterruptedException {

        ArrayList<String> inLinks = new ArrayList<String>();
        PageRankData pageRankData = new PageRankData();
        for (PageRankData value : values) {
            if (value.inLinks != null && value.inLinks.size() > 0) {
                inLinks.addAll(value.inLinks);
            }
            if (value.pageRank > pageRankData.pageRank) {
                pageRankData.pageRank = value.pageRank;
                pageRankData.outLinks = value.outLinks;
            }
        }

        double newPageRank = 0.0;
        for (String inLink : inLinks) {
            double outLinkCount = pageRankData.outLinks.size();
            newPageRank += DAMPING_FACTOR * (pageRankData.pageRank / outLinkCount);
        }
        newPageRank += (1 - DAMPING_FACTOR);

        pageRankData.pageRank = newPageRank;
        pageRankData.inLinks = inLinks;

        context.write(key, pageRankData);
        System.out.println("Key: " + key);
        System.out.println("PageRankData: " + pageRankData);
        System.out.println("In-Links: " + inLinks);
        System.out.println("Out-Links: " + pageRankData.outLinks);
    }
}

