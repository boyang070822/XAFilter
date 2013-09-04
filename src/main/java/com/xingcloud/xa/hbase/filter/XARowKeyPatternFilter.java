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
import com.xingcloud.xa.hbase.util.rowkeyCondition.RowKeyFilterCondition;
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

    private List<RowKeyFilterCondition> conditions=null;
    private int conditionIndex=0;
    private boolean filterOutRow = false;

    public XARowKeyPatternFilter() {
        super();
    }

    public XARowKeyPatternFilter(List<RowKeyFilterCondition> conditions){
        super();
        this.conditions=conditions;
    }



    @Override
    public void reset() {
        this.filterOutRow = false;
    }


    @Override
    public ReturnCode filterKeyValue(KeyValue kv) {
        if (this.filterOutRow) {
                if(this.conditionIndex==this.conditions.size())
                    return ReturnCode.NEXT_ROW;
                return ReturnCode.SEEK_NEXT_USING_HINT;
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRowKey(byte[] data, int offset, int length) {
        byte[] rk = Arrays.copyOfRange(data, offset, offset + length);
        if(conditions!=null){
            if(conditions.get(conditionIndex).accept(rk)!=0)
                this.filterOutRow=true;
            return this.filterOutRow;
        }
        return this.filterOutRow;
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue kv) {
        byte[] rk = kv.getRow();
        resetIndex();
        while(conditionIndex<this.conditions.size()){
            RowKeyFilterCondition condition=this.conditions.get(conditionIndex);
            //byte[] rkPart=Arrays.copyOf(rk,pattern.length);
            boolean aceeptCondition=false;
            if(condition.accept(rk)<=0)
                aceeptCondition=true;
            if(aceeptCondition){
                KeyValue newKV = new KeyValue(condition.getStartRk(), kv.getFamily(), kv.getQualifier());
                /*
                LOG.info("pattern "+Bytes.toString(condition.getStartRk()));
                LOG.info("rk "+Bytes.toString(rk));
                LOG.info("bigPattern ");
                LOG.info("conditionIndex "+conditionIndex);
                */
                return KeyValue.createFirstOnRow(newKV.getBuffer(), newKV.getRowOffset(), newKV
                        .getRowLength(), newKV.getBuffer(), newKV.getFamilyOffset(), newKV
                        .getFamilyLength(), null, 0, 0);
            }
            conditionIndex++;
        }
        byte[] result=increaseFirstByte(this.conditions.get(conditionIndex-1).getEndRk());
        KeyValue newKV=new KeyValue(result,kv.getFamily(),kv.getQualifier());
        /*
        LOG.info("increase Result "+Bytes.toString(result));
        LOG.info("conditionIndex "+conditionIndex);
        */
        return KeyValue.createFirstOnRow(newKV.getBuffer(), newKV.getRowOffset(), newKV
                    .getRowLength(), newKV.getBuffer(), newKV.getFamilyOffset(), newKV
                    .getFamilyLength(), null, 0, 0);

    }

    public static byte[] increaseFirstByte(byte[] orig){
        byte[] result=new byte[orig.length];
        result[0]=(byte)(orig[0]+1);
        for(int i=1;i<orig.length;i++){
            result[i]=orig[i];
        }
        return result;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //LOG.info("Read fields of XARowKeyConditonFilter...");
        int size = in.readInt();
        //LOG.info("Patterns size: " + size);
        List<RowKeyFilterCondition> conditions = new ArrayList<RowKeyFilterCondition>(size);
        for (int i = 0; i < size; i++) {
            String conditionType = new String(Bytes.readByteArray(in));
            //LOG.info("condition type " + conditionType);
            try {
                Class conditionClass=Class.forName(conditionType);
                RowKeyFilterCondition condition=(RowKeyFilterCondition)conditionClass.newInstance();
                condition.readFields(in);
                conditions.add(condition);
            } catch (Exception e) {
                e.printStackTrace();
                throw  new IOException(e);
            }
        }
        this.conditions=conditions;
        this.filterOutRow = false;
        this.conditionIndex=0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(conditions.size());
        for (RowKeyFilterCondition condition : conditions) {
            //LOG.info("Write pattern: " + condition);
            condition.write(out);
        }
    }

    private void resetIndex() {
        conditionIndex=0;
    }
}

