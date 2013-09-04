package com.xingcloud.xa.hbase.util.rowkeyCondition;

import com.xingcloud.xa.hbase.util.ByteUtils;
import com.xingcloud.xa.hbase.util.Condition;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/4/13
 * Time: 2:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class  RowKeyFilterPattern implements RowKeyFilterCondition {
    public static Logger logger= LoggerFactory.getLogger(RowKeyFilterPattern.class);
    private byte[] pattern;
    public RowKeyFilterPattern(){

    }
    public RowKeyFilterPattern(String pattern){
        this.pattern= ByteUtils.toBytesBinary(pattern);
    }

    public void readFields(DataInput in) throws IOException {
        pattern=Bytes.readByteArray(in);
        //logger.info("pattern "+Bytes.toStringBinary(pattern));
    }
    public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out,Bytes.toBytes(this.getClass().getName()));
        Bytes.writeByteArray(out,pattern);
    }

    @Override
    public int accept(byte[] rk) {
        if(Bytes.startsWith(rk, pattern))
            return 0;
        //logger.info("not accept "+Bytes.toStringBinary(rk)+"  "+Bytes.toStringBinary(pattern));
        if(Bytes.compareTo(rk,pattern)<0)
            return -1;
        return 1;
    }
    @Override
    public byte[] getStartRk() {
        return pattern;
    }

    @Override
    public byte[] getEndRk() {
        return ByteUtils.binaryIncrementPos(pattern,1l);
    }


}
