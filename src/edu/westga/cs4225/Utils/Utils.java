package edu.westga.cs4225.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.westga.cs4225.PageRank.PageRankData;

import org.apache.hadoop.conf.Configuration;

public class Utils {

    public static ArrayList<String> extractLinks(String text) {
        ArrayList<String> links = new ArrayList<>();
        Pattern pattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            links.add(matcher.group(1));
        }
        return links;
    }

    public static double calculatePageRankDifference(Path prevOutputPath, Path currentOutputPath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
    
        FileStatus[] prevFileStatus = fs.listStatus(prevOutputPath, path -> path.getName().startsWith("part-r-"));
        FileStatus[] currentFileStatus = fs.listStatus(currentOutputPath, path -> path.getName().startsWith("part-r-"));
    
        double sumDifference = 0.0;
    
        for (int i = 0; i < prevFileStatus.length; i++) {
            Path prevInputFile = prevFileStatus[i].getPath();
            Path currentInputFile = currentFileStatus[i].getPath();
    
            try (BufferedReader prevBr = new BufferedReader(new InputStreamReader(fs.open(prevInputFile)));
                 BufferedReader currentBr = new BufferedReader(new InputStreamReader(fs.open(currentInputFile)))) {
                String prevLine;
                String currentLine;
                while ((prevLine = prevBr.readLine()) != null && (currentLine = currentBr.readLine()) != null) {
                    String[] prevTokens = prevLine.split("\t");
                    String[] currentTokens = currentLine.split("\t");
    
                    if (prevTokens.length < 2 || currentTokens.length < 2) {
                        continue;
                    }
    
                    PageRankData prevPageRankData = fromString(prevTokens[1]);
                    PageRankData currentPageRankData = fromString(currentTokens[1]);
    
                    sumDifference += Math.abs(prevPageRankData.pageRank - currentPageRankData.pageRank);
                }
            }
        }
    
        return sumDifference;
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

    public static void normalizePageRanks(Path inputPath, Path outputPath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
    
        Path inputFile = findInputFile(fs, inputPath);
        double sumPageRanks = calculateSumOfPageRanks(fs, inputFile);
    
        normalizeAndWritePageRanks(fs, inputFile, outputPath, sumPageRanks);
    }
    
    private static Path findInputFile(FileSystem fs, Path inputPath) throws IOException {
        FileStatus[] fileStatus = fs.listStatus(inputPath, path -> path.getName().startsWith("part-r-"));
        if (fileStatus.length == 0) {
            throw new FileNotFoundException("No part-r- file found in the input directory.");
        }
        return fileStatus[0].getPath();
    }
    
    private static double calculateSumOfPageRanks(FileSystem fs, Path inputFile) throws IOException {
        double sumPageRanks = 0.0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputFile)))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                if (tokens.length < 2) {
                    continue;
                }
                PageRankData pageRankData = fromString(tokens[1]);
                sumPageRanks += pageRankData.pageRank;
            }
        }
        return sumPageRanks;
    }
    
    private static void normalizeAndWritePageRanks(FileSystem fs, Path inputFile, Path outputPath, double sumPageRanks) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputFile)));
             BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputPath, "normalized-ranks"))))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                if (tokens.length < 2) {
                    continue;
                }
                PageRankData pageRankData = fromString(tokens[1]);
                pageRankData.pageRank = pageRankData.pageRank / sumPageRanks;    
  
                bw.write(pageRankData.pageTitle + "\t" + pageRankData.toString());
                bw.newLine();
            }
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
            pageRankData.outLinks = outLinksString.isEmpty() ? new ArrayList<String>() : new ArrayList<>(Arrays.asList(outLinksString.split(", ")));
        }
    
        return pageRankData;
    }
    
}
