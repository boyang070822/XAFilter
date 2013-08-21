package com.xingcloud.xa.hbase.filter;

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
        XARowKeyFilter filter=new XARowKeyFilter("age_deu","pay.*",days);
        System.out.println("create filter succuess");
    }
}
