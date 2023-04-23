package edu.westga.cs4225.PageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.westga.cs4225.Utils.DatasetParser;

import java.io.IOException;
import java.util.List;

public class PageRankMapper extends Mapper<Object, Text, Text, PageRankData> {
    private Text pageTitle = new Text();
    private DatasetParser parser = new DatasetParser();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("Mapper input value: " + value);
        System.out.println("Mapper input key: " + key);

        List<PageRankData> pageRankDataList;
        int currentIteration = context.getConfiguration().getInt("iteration", 0);
        if (currentIteration == 0) {
            pageRankDataList = parser.parse(value.toString());
        } else {
            pageRankDataList = parser.parseOutput(value.toString());
        }

        for (PageRankData data : pageRankDataList) {
            pageTitle.set(data.pageTitle);
            data.pageRank = 1.0; // Initialize the PageRank value for each page
            context.write(pageTitle, data);

            double outLinkPageRank = data.pageRank / data.outLinks.length;

            for (String link : data.outLinks) {
                pageTitle.set(link);
                PageRankData outLinkData = new PageRankData();
                outLinkData.pageTitle = "";
                outLinkData.pageRank = outLinkPageRank;
                outLinkData.outLinks = new String[] {};
                context.write(pageTitle, outLinkData);
            }
        }
    }
}
