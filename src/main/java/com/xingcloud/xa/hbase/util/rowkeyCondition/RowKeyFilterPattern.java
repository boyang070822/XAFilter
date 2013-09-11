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
public class  RowKeyFilterPattern implements RowKeyFilterCondition,Comparable<RowKeyFilterCondition> {
    public static Logger logger= LoggerFactory.getLogger(RowKeyFilterPattern.class);
    private byte[] pattern;
    private byte[] tailSrt;
    private byte[] tailEnd;
    private boolean sampling=false;
    //private boolean hasCompleteRange=false;
    private byte[] srk;
    private byte[] enk;
    private byte[] destination;
    public RowKeyFilterPattern(){

    }
    public RowKeyFilterPattern(String pattern){
        this.pattern= ByteUtils.toBytesBinary(pattern);
        this.tailSrt=new byte[]{0};
        this.tailEnd=new byte[]{-1};
    }

    public RowKeyFilterPattern(String pattern,String tailSrt, String tailEnd)  {
        this.pattern=Bytes.toBytesBinary(pattern);
        this.tailSrt=Bytes.toBytesBinary(tailSrt);
        this.tailEnd=Bytes.toBytesBinary(tailEnd);
        assert (this.tailSrt.length!=this.tailEnd.length);
    }

    public void readFields(DataInput in) throws IOException {
        pattern=Bytes.readByteArray(in);
        tailSrt=Bytes.readByteArray(in);
        tailEnd=Bytes.readByteArray(in);
        int i=0;
        while(i<tailSrt.length){
            if(!(tailSrt[i]==(byte)0&&tailEnd[i]==(byte)-1)){
                sampling=true;
                break;
            }
            i++;
        }
        if(sampling){
            if(Bytes.equals(Arrays.copyOfRange(pattern,pattern.length-2,pattern.length),new byte[]{'.',-1})){
                srk=Bytes.add(pattern,tailSrt);
                enk=Bytes.add(pattern,tailEnd);
            }else{
                srk=Bytes.add(pattern,new byte[]{'.',-1},tailSrt);
                enk=Bytes.add(pattern,new byte[]{'.',-1},tailEnd);
            }
            logger.info("srk "+Bytes.toStringBinary(srk));
            logger.info("enk "+Bytes.toStringBinary(enk));
            destination=srk;
        }else {
            destination=pattern;
        }
        logger.info("pattern "+Bytes.toStringBinary(pattern));

    }
    public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out,Bytes.toBytes(this.getClass().getName()));
        Bytes.writeByteArray(out,pattern);
        Bytes.writeByteArray(out,tailSrt);
        Bytes.writeByteArray(out,tailEnd);
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof RowKeyFilterPattern){
            RowKeyFilterPattern refPattern=(RowKeyFilterPattern)o;
            if(Bytes.equals(refPattern.getStartRk(), this.pattern))
                return true;
        }
        return false;
    }

    @Override
    public boolean accept(byte[] rk) {
        if(sampling){
            if(Bytes.compareTo(rk,srk)>=0&&Bytes.compareTo(rk,enk)<0)
                return true;
            return false;
        }else {
            if(Bytes.startsWith(rk,pattern))
                return true;
            return false;
        }
    }

    @Override
    public int rkCompareTo(byte[] rk){
        if(sampling){
            if(Bytes.compareTo(rk,srk)>=0&&Bytes.compareTo(rk,enk)<0)
                return 0;
            else if(Bytes.compareTo(rk,srk)<0)
                return -1;
            else
                return 1;
        }else {
            if(Bytes.startsWith(rk,pattern))
                return 0;
            else if(Bytes.compareTo(rk,pattern)<0)
                return -1;
            else
                return 1;
        }
    }
    @Override
    public byte[] getStartRk() {
        return pattern;
    }

    @Override
    public byte[] getEndRk() {
        return ByteUtils.binaryIncrementPos(pattern,1l);
    }

    @Override
    public byte[] getDestination(){
        return destination;
    }


    @Override
    public int compareTo(RowKeyFilterCondition o) {
        return Bytes.compareTo(this.getStartRk(),o.getStartRk());
    }
}
