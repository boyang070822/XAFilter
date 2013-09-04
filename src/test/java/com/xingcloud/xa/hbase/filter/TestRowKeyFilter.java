package com.xingcloud.xa.hbase.filter;

import com.xingcloud.xa.mongodb.MongoDBOperation;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/21/13
 * Time: 6:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestRowKeyFilter {

    @Test
    public void createRowKeyFilter() throws IOException {
        List<String> days=new ArrayList<String>();
        days.add("20130801");
        //XARowKeyFilter filter=new XARowKeyFilter("sof-dsk_deu","visit.*.heartbeat",days);
        String pattern1="20130101visit.budit.";
        String pattern2="20130101visit.click.";
        byte[] p1= Bytes.toBytes(pattern1);
        byte[] p2= Bytes.toBytes(pattern2);
        System.out.println("p1 greater than p2 "+Bytes.compareTo(p1,p2));

        System.out.println("create filter succuess");
    }
}
