/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/23/13
 * Time: 6:43 PM
 * To change this template use File | Settings | File Templates.
 */
package com.xingcloud.xa.hbase.filter;

import com.xingcloud.xa.hbase.util.ByteUtils;
import com.xingcloud.xa.hbase.util.HBaseEventUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;


public class XARowKeyPatternFilter extends FilterBase {
    private static Logger LOG = LoggerFactory.getLogger(XARowKeyFilter.class);

    private List<byte[]> patternBytes=null;
    private List<String> patterns=null;
    private int patternIndex=0;
    private boolean filterOutRow = false;
    private long warningCounter = 0;

    public XARowKeyPatternFilter() {
        super();
    }

    public XARowKeyPatternFilter(List<String> partterns){
        super();
        this.patternBytes=new ArrayList<>();
        this.patterns=partterns;
        for(String pattern: partterns){
            byte[] patternOfBytes= ByteUtils.toBytesBinary(pattern);
            patternBytes.add(patternOfBytes);
        }

    }



    @Override
    public void reset() {
        this.filterOutRow = false;
    }


    @Override
    public ReturnCode filterKeyValue(KeyValue kv) {
        if (this.filterOutRow) {
                if(this.patternIndex==this.patternBytes.size())
                    return ReturnCode.NEXT_ROW;
                return ReturnCode.SEEK_NEXT_USING_HINT;
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRowKey(byte[] data, int offset, int length) {
        byte[] rowKeyByteArray = Arrays.copyOfRange(data, offset, offset + length);
        if(patternBytes!=null){
            if(!Bytes.startsWith(rowKeyByteArray,patternBytes.get(patternIndex)))
                this.filterOutRow=true;
            return this.filterOutRow;
        }
        return this.filterOutRow;
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue kv) {
        byte[] rk = kv.getRow();
        resetIndex();
        while(patternIndex<this.patternBytes.size()){
            byte[] pattern=this.patternBytes.get(patternIndex);
            byte[] rkPart=Arrays.copyOf(rk,pattern.length);
            boolean bigPattern=false;
            for(int i=0;i<pattern.length;i++){
                if(pattern[i]>rkPart[i])
                {
                    bigPattern=true;
                    break;
                }
            }
            if(bigPattern){
                KeyValue newKV = new KeyValue(pattern, kv.getFamily(), kv.getQualifier());
                return KeyValue.createFirstOnRow(newKV.getBuffer(), newKV.getRowOffset(), newKV
                        .getRowLength(), newKV.getBuffer(), newKV.getFamilyOffset(), newKV
                        .getFamilyLength(), null, 0, 0);
            }
            patternIndex++;
        }
        return kv;

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        LOG.info("Read fields of XARowKeyPatternFilter...");
        int size = in.readInt();
        LOG.info("Patterns size: " + size);
        List<String> patterns = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            String pattern = new String(Bytes.readByteArray(in));
            LOG.info("Read pattern " + pattern);
            patterns.add(pattern);
        }
        this.patterns=patterns;
        this.patternBytes=new ArrayList<>();
        for(String pattern: patterns){
            byte[] patternOfBytes= ByteUtils.toBytesBinary(pattern);
            patternBytes.add(patternOfBytes);
        }
        this.filterOutRow = false;
        this.patternIndex=0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(patterns.size());
        for (String pattern : patterns) {
            LOG.info("Write pattern: " + pattern);
            Bytes.writeByteArray(out, Bytes.toBytes(pattern));
        }
    }



    private void resetIndex() {
        patternIndex=0;
    }
}

