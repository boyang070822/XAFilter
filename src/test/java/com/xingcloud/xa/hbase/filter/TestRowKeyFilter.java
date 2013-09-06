package com.xingcloud.xa.hbase.filter;

import com.xingcloud.xa.hbase.util.rowkeyCondition.RowKeyFilterCondition;
import com.xingcloud.xa.hbase.util.rowkeyCondition.RowKeyFilterPattern;
import com.xingcloud.xa.hbase.util.rowkeyCondition.RowKeyFilterRange;
import com.xingcloud.xa.mongodb.MongoDBOperation;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
        RowKeyFilterPattern pattern1=new RowKeyFilterPattern("20130101visit.budit.\\xFF");
        RowKeyFilterPattern pattern2=new RowKeyFilterPattern("20130101visit.click.\\xFF");
        RowKeyFilterPattern pattern3=new RowKeyFilterPattern("20130101visit.click.\\xFF");
        RowKeyFilterRange   range1= new RowKeyFilterRange("20130101visit.visu.\\xFF","20130101visit.\\xFF");
        RowKeyFilterPattern pattern4=new RowKeyFilterPattern("20130101pay.complete.\\xFF");
        XARowKeyPatternFilter filter=new XARowKeyPatternFilter(Arrays.asList(new RowKeyFilterCondition[]{pattern2,pattern1,pattern3,range1,pattern4}));
        //System.out.println("p1 greater than p2 "+Bytes.compareTo(p1,p2));

        System.out.println("create filter succuess");
    }
}
