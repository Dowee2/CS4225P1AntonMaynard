package edu.westga.cs4225.PageRank;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class PageRankData implements Writable {
    public String pageTitle;
    public double pageRank;
    public String[] outLinks;

    public PageRankData(String pageTitle, double pageRank, String[] outLinks) {
        this.pageTitle = pageTitle;
        this.pageRank = pageRank;
        this.outLinks = outLinks;
    }

    public PageRankData() {
        this.pageTitle = "";
        this.pageRank = 0.0;
        this.outLinks = new String[] {};
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(pageTitle);
        out.writeDouble(pageRank);
        out.writeInt(outLinks.length);
        for (String link : outLinks) {
            out.writeUTF(link);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pageTitle = in.readUTF();
        pageRank = in.readDouble();
        int numLinks = in.readInt();
        outLinks = new String[numLinks];
        for (int i = 0; i < numLinks; i++) {
            outLinks[i] = in.readUTF();
        }
    }
    
    @Override
    public String toString() {
        return "PageRankData{" +
                "pageTitle='" + pageTitle + '\'' +
                ", pageRank=" + pageRank +
                ", outLinks=" + Arrays.toString(outLinks) +
                '}';
    }

}
