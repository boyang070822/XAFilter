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
    private boolean sampling;
    private byte[] destination;
    public RowKeyFilterPattern(){

    }
    public RowKeyFilterPattern(String pattern){
        this.pattern= ByteUtils.toBytesBinary(pattern);
        this.tailSrt=new byte[]{0};
        this.tailEnd=new byte[]{-1};
    }

    public RowKeyFilterPattern(String pattern,String tailSrt, String tailEnd){
        this.pattern=Bytes.toBytesBinary(pattern);
        this.tailSrt=Bytes.toBytesBinary(tailSrt);
        this.tailEnd=Bytes.toBytesBinary(tailEnd);
    }

    public void readFields(DataInput in) throws IOException {
        pattern=Bytes.readByteArray(in);
        tailSrt=Bytes.readByteArray(in);
        tailEnd=Bytes.readByteArray(in);
        if((tailSrt.length==1&&tailSrt[0]==(byte)0)&&(tailEnd.length==1&&tailEnd[0]==(byte)-1))
            this.sampling=false;
        else
            this.sampling=true;
        this.destination=Bytes.add(this.pattern,this.tailSrt);
        //logger.info("pattern "+Bytes.toStringBinary(pattern));
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
        if(Bytes.startsWith(rk, pattern)){
            if(!sampling)
                return true;
            byte[] rkTailSrt= Arrays.copyOfRange(rk,rk.length-tailSrt.length,rk.length);
            byte[] rkTailEnd= Arrays.copyOfRange(rk,rk.length-tailEnd.length,rk.length);
            if(Bytes.compareTo(rkTailSrt,tailSrt)>=0&&Bytes.compareTo(rkTailEnd,tailEnd)<0){
                logger.info("rk: "+Bytes.toStringBinary(rk)+",rkTail: "+
                        Bytes.toStringBinary(rkTailSrt)+"  accept ");
                return true;
            }
        }
        logger.info("not accept "+Bytes.toStringBinary(rk)+"  "+Bytes.toStringBinary(pattern));
        return false;
    }

    @Override
    public int rkCompareTo(byte[] rk){
        if(Bytes.startsWith(rk, pattern)){
            byte[] rkHead=Arrays.copyOf(rk,rk.length-tailSrt.length);
            byte[] nextHead=Bytes.incrementBytes(rkHead,1l);
            destination=Bytes.add(nextHead,tailSrt);
            return 0;
        }
        logger.info("not accept "+Bytes.toStringBinary(rk)+"  "+Bytes.toStringBinary(pattern));
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

    @Override
    public byte[] getDestination(){
        return destination;
    }


    @Override
    public int compareTo(RowKeyFilterCondition o) {
        return Bytes.compareTo(this.getStartRk(),o.getStartRk());
    }
}
