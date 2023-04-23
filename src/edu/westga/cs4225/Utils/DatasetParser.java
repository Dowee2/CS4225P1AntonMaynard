package edu.westga.cs4225.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.westga.cs4225.PageRank.PageRankData;

public class DatasetParser {
    private static final Pattern PAGE_PATTERN = Pattern.compile("<page>.*?<\\/page>", Pattern.DOTALL);
    private static final Pattern TITLE_PATTERN = Pattern.compile("<title>(.*?)<\\/title>");
    private static final Pattern TEXT_PATTERN = Pattern.compile("<text>(.*?)<\\/text>", Pattern.DOTALL);
    private static final Pattern LINK_PATTERN = Pattern.compile("\\[\\[(.*?)\\]\\]");

    public List<PageRankData> parse(String input) {
        List<PageRankData> pageRankDataList = new ArrayList<>();

        Matcher pageMatcher = PAGE_PATTERN.matcher(input);
        while (pageMatcher.find()) {
            String pageContent = pageMatcher.group();
            PageRankData pageRankData = new PageRankData("",0.0,null);

            Matcher titleMatcher = TITLE_PATTERN.matcher(pageContent);
            if (titleMatcher.find()) {
                pageRankData.pageTitle = titleMatcher.group(1);
            }

            Matcher textMatcher = TEXT_PATTERN.matcher(pageContent);
            if (textMatcher.find()) {
                String textContent = textMatcher.group(1);
                Matcher linkMatcher = LINK_PATTERN.matcher(textContent);
                List<String> outLinks = new ArrayList<>();
                while (linkMatcher.find()) {
                    outLinks.add(linkMatcher.group(1));
                }
                pageRankData.outLinks = outLinks.toArray(new String[0]);
            }
            System.out.println("Parsed data: " + pageRankData);
            pageRankDataList.add(pageRankData);
        }

        return pageRankDataList;
    }

    public List<PageRankData> parseOutput(String line) {
        List<PageRankData> pageRankDataList = new ArrayList<>();
    
        String[] keyValue = line.split("\t");
        String pageTitle = keyValue[0];
        String pageData = keyValue[1];
    
        Pattern pattern = Pattern.compile("PageRankData\\{pageTitle='(.+)', pageRank=(.+), outLinks=\\[(.*)\\]\\}");
        Matcher matcher = pattern.matcher(pageData);
    
        if (matcher.find()) {
            String outLinksString = matcher.group(3);
            String[] outLinks = outLinksString.isEmpty() ? new String[]{} : outLinksString.split(", ");
            double pageRank = Double.parseDouble(matcher.group(2));
            pageRankDataList.add(new PageRankData(pageTitle, pageRank, outLinks));
        }
    
        return pageRankDataList;
    }
    
}
