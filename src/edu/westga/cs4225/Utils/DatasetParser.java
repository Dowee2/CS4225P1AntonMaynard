package edu.westga.cs4225.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.westga.cs4225.PageRank.PageRankData;

public class DatasetParser {

    private static final Pattern TITLE_PATTERN = Pattern.compile("<title>(.*?)</title>");
    private static final Pattern TEXT_PATTERN = Pattern.compile("<text(.*?)>(.*?)</text>");
    private static final Pattern LINK_PATTERN = Pattern.compile("\\[\\[(.*?)\\]\\]");

    public static Map<String, List<String>> generateAdjacencyList(String xmlString) {
        Map<String, List<String>> adjacencyList = new HashMap<>();
        String[] pageStrings = xmlString.split("<page>");
        for (String pageString : pageStrings) {
            if (pageString.trim().startsWith("<title>")) {
                String title = extractTitle(pageString);
                String text = extractText(pageString);
                List<String> outLinks = extractLinks(text);
                adjacencyList.put(title, outLinks);
            }
        }
        return adjacencyList;
    }

    public static List<PageRankData> parse(String xmlString) {
        List<PageRankData> pages = new ArrayList<>();
        String[] pageStrings = xmlString.split("<page>");
        for (String pageString : pageStrings) {
            if (pageString.trim().startsWith("<title>")) {
                String title = extractTitle(pageString);
                String text = extractText(pageString);
                List<String> outLinks = extractLinks(text);
                pages.add(new PageRankData(title, 0.0, outLinks, new ArrayList<String>(), 0.0));
            }
        }
        return pages;
    }

    private static String extractTitle(String pageString) {
        Matcher titleMatcher = TITLE_PATTERN.matcher(pageString);
        if (titleMatcher.find()) {
            return titleMatcher.group(1);
        } else {
            throw new IllegalArgumentException("Invalid page string: " + pageString);
        }
    }

    private static String extractText(String pageString) {
        Matcher textMatcher = TEXT_PATTERN.matcher(pageString);
        if (textMatcher.find()) {
            return textMatcher.group(2);
        } else {
            throw new IllegalArgumentException("Invalid page string: " + pageString);
        }
    }

    private static List<String> extractLinks(String text) {
        List<String> links = new ArrayList<>();
        Matcher linkMatcher = LINK_PATTERN.matcher(text);
        while (linkMatcher.find()) {
            String link = linkMatcher.group(1);
            String[] linkParts = link.split("\\|");
            links.add(linkParts[0]);
        }
        return links;
    }

    public List<PageRankData> parseOutput(String line) {
        List<PageRankData> pageRankDataList = new ArrayList<>();

        String[] keyValue = line.split("\t");
        String pageTitle = keyValue[0];
        String pageData = keyValue[1];

        Pattern pattern = Pattern.compile("PageRankData\\{pageTitle='(.+)', pageRank=(.+), outLinks=\\[(.*)\\], inLinks=\\[(.*)\\]\\}");
        Matcher matcher = pattern.matcher(pageData);

        if (matcher.find()) {
            String outLinksString = matcher.group(3);
            ArrayList<String> outLinks = outLinksString.isEmpty() ? new ArrayList<String>() : new ArrayList<>(Arrays.asList(outLinksString.split(", ")));
            String inLinksString = matcher.group(4);
            ArrayList<String> inLinks = inLinksString.isEmpty() ? new ArrayList<String>() : new ArrayList<>(Arrays.asList(inLinksString.split(", ")));
            double pageRank = Double.parseDouble(matcher.group(2));
            ArrayList<String> adjacencyList = new ArrayList<String>();
            for (String link : outLinks) {
                adjacencyList.add(link);
            }
            pageRankDataList.add(new PageRankData(pageTitle, pageRank, outLinks, inLinks, adjacencyList));
        }

        return pageRankDataList;
    }
}

