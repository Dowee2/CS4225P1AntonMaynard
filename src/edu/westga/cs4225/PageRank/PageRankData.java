package edu.westga.cs4225.PageRank;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PageRankData implements Writable {
    public String pageTitle;
    public double pageRank;
    public ArrayList<String> adjacencyList;
    public double newPageRank;

    public PageRankData(String pageTitle, double pageRank, ArrayList<String> adjacencyList, double newPageRank) {
        this.pageTitle = pageTitle;
        this.pageRank = pageRank;
        this.adjacencyList = adjacencyList;
        this.newPageRank = newPageRank;
    }

    public PageRankData(String pageTitle, double pageRank, ArrayList<String> adjacencyList) {
        this.pageTitle = pageTitle;
        this.pageRank = pageRank;
        this.adjacencyList = adjacencyList;
    }

    public PageRankData() {
        this.pageTitle = "";
        this.pageRank = 0.0;
        this.adjacencyList = new ArrayList<String>();
        this.newPageRank = 0.0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(pageTitle);
        out.writeDouble(pageRank);
        out.writeInt(adjacencyList.size());
        for (String link : adjacencyList) {
            out.writeUTF(link);
        }
        out.writeDouble(newPageRank);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pageTitle = in.readUTF();
        pageRank = in.readDouble();
        int numAdjacencyList = in.readInt();
        adjacencyList = new ArrayList<String>();
        for (int i = 0; i < numAdjacencyList; i++) {
            adjacencyList.add(in.readUTF());
        }
        newPageRank = in.readDouble();
    }

    @Override
    public String toString() {
        return "PageRankData{" +
                "pageTitle='" + pageTitle + '\'' +
                ", pageRank=" + pageRank +
                ", adjacencyList=" + Arrays.toString(adjacencyList.toArray()) +
                ", newPageRank=" + newPageRank +
                '}';
    }
}
