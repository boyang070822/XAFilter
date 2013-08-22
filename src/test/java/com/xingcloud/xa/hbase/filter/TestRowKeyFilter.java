package com.xingcloud.xa.hbase.filter;

import com.xingcloud.xa.mongodb.MongoDBOperation;
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
        XARowKeyFilter filter=new XARowKeyFilter("sof-dsk_deu","visit.*.heartbeat",days);
        /*
        Thread[] threads=new Thread[500];
        for(int i=0;i<threads.length;i++){
            threads[i]=new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        MongoDBOperation.getEventSet("age_deu","pay.*");
                    } catch (IOException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }
            });
            threads[i].start();
            threads[i].run();
        }
        */
        System.out.println("create filter succuess");
    }
}
