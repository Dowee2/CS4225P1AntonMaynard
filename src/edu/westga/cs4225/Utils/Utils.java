package edu.westga.cs4225.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.westga.cs4225.PageRank.PageRankData;

import org.apache.hadoop.conf.Configuration;

public class Utils {

    public static double calculatePageRankDifference(Path prevOutputPath, Path outputPath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
    
        BufferedReader prevOutputReader = new BufferedReader(new InputStreamReader(fs.open(new Path(prevOutputPath, "part-r-00000"))));
        BufferedReader currentOutputReader = new BufferedReader(new InputStreamReader(fs.open(new Path(outputPath, "part-r-00000"))));
    
        double totalDifference = 0.0;
        String prevLine, currentLine;
    
        while ((prevLine = prevOutputReader.readLine()) != null && (currentLine = currentOutputReader.readLine()) != null) {
            String[] prevTokens = prevLine.split("\t");
            String[] currentTokens = currentLine.split("\t");
    
            double prevPageRank = Double.parseDouble(prevTokens[1].split(",")[1].split("=")[1]);
            double currentPageRank = Double.parseDouble(currentTokens[1].split(",")[1].split("=")[1]);
    
            totalDifference += Math.abs(currentPageRank - prevPageRank);
        }
    
        prevOutputReader.close();
        currentOutputReader.close();
    
        return totalDifference;
    }

    public static double findMaxPageRank(Path outputPath) throws IOException {
        double maxPageRank = 0;
        FileSystem fs = outputPath.getFileSystem(new Configuration());
        FileStatus[] fileStatus = fs.listStatus(outputPath, path -> path.getName().startsWith("part-r-"));
    
        for (FileStatus status : fileStatus) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split("\\t");
                if (tokens.length == 2) {
                    PageRankData pageRankData = fromString(tokens[1]);
                    maxPageRank = Math.max(maxPageRank, pageRankData.pageRank);
                }
            }
            reader.close();
        }
            return maxPageRank;
    }

    public static void normalizePageRank(Path inputPath, Path outputPath, double maxPageRank) throws IOException {
        FileSystem fs = inputPath.getFileSystem(new Configuration());
        FileStatus[] fileStatus = fs.listStatus(inputPath, path -> path.getName().startsWith("part-r-"));
    
        for (FileStatus status : fileStatus) {
            Path outputFilePath = new Path(outputPath, status.getPath().getName());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputFilePath)));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split("\\t");
                if (tokens.length == 2) {
                    String pageTitle = tokens[0];
                    PageRankData pageRankData = fromString(tokens[1]);
                    pageRankData.pageRank /= maxPageRank;
                    writer.write(pageTitle + "\t" + pageRankData.toString());
                    writer.newLine();
                }
            }
            reader.close();
            writer.close();
        }
    }
    
    public static PageRankData fromString(String input) {
        PageRankData pageRankData = new PageRankData();
    
        Pattern pattern = Pattern.compile("PageRankData\\{pageTitle='(.*?)', pageRank=(.*?), outLinks=\\[(.*?)\\]\\}");
        Matcher matcher = pattern.matcher(input);
    
        if (matcher.find()) {
            pageRankData.pageTitle = matcher.group(1);
            pageRankData.pageRank = Double.parseDouble(matcher.group(2));
    
            String outLinksString = matcher.group(3);
            if (!outLinksString.isEmpty()) {
                pageRankData.outLinks = outLinksString.split(", ");
            } else {
                pageRankData.outLinks = new String[]{};
            }
        }
    
        return pageRankData;
    }
    
}
