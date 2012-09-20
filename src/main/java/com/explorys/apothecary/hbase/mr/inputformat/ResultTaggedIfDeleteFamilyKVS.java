/* Copyright 2012 Explorys, Inc                                                                                                                                                                                                                
 *                                                                                                                                                                                                                                          
 *     Licensed under the Apache License, Version 2.0 (the "License");                                                                                                                                                                      
 *   you may not use this file except in compliance with the License.                                                                                                                                                                       
 *   You may obtain a copy of the License at                                                                                                                                                                                                
 *                                                                                                                                                                                                                                          
 *       http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                                                                         
 *                                                                                                                                                                                                                                          
 *       Unless required by applicable law or agreed to in writing, software                                                                                                                                                                
 *       distributed under the License is distributed on an "AS IS" BASIS,                                                                                                                                                                  
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.                                                                                                                                                           
 *   See the License for the specific language governing permissions and                                                                                                                                                                    
 *   limitations under the License.                                                                                                                                                                                                         
 */
package com.explorys.apothecary.hbase.mr.inputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.WritableWithSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class ResultTaggedIfDeleteFamilyKVS extends Result implements Writable, WritableWithSize {

    boolean hasDeletes;
    
    /**
     * Writable constructor: DO NOT USE
     */
    public ResultTaggedIfDeleteFamilyKVS(){}
    
    public ResultTaggedIfDeleteFamilyKVS(KeyValue[] kvs, boolean hasDeletes)
    {
        super(kvs);
        this.hasDeletes = hasDeletes;
    }
    
    //TODO fix this to have good delete logic
    @Override
    public List<KeyValue> getColumn(byte [] family, byte [] qualifier) {
        List<KeyValue> result = new ArrayList<KeyValue>();

        KeyValue [] kvs = raw();

        if (kvs == null || kvs.length == 0) {
          return result;
        }
        int pos = binarySearch(kvs, family, qualifier);
        if (pos == -1) {
          return result; // cant find it
        }

        for (int i = pos ; i < kvs.length ; i++ ) {
          KeyValue kv = kvs[i];
          if (kv.matchingColumn(family,qualifier)) {
            result.add(kv);
          } else {
            break;
          }
        }

        return result;
      }
    
    
    @Override
    public KeyValue getColumnLatest(byte [] family, byte [] qualifier) {
        KeyValue [] kvs = raw(); // side effect possibly.
        if (kvs == null || kvs.length == 0) {
          return null;
        }
        
        int pos = binarySearch(kvs, family, qualifier);
        if (pos == -1) {
            return null;
        }
        if (!hasDeletes){
            KeyValue kv = kvs[pos];
            if (kv.matchingColumn(family, qualifier)) {
                return checkLengthAndReturn(kv);
            }
            return null;
        }
        else{
            KeyValue kv = kvs[0];
            KeyValue firstInFamily = createFirstOnFamily(family, kv);
            int famPos = binarySearchForFamily(kvs, firstInFamily);
            
            KeyValue famKV = kvs[famPos];
            if ( famKV.isDeleteFamily() && famKV.matchingFamily(family))
                            {
                    kv = kvs[pos];
                    if (kv.matchingColumn(family, qualifier)) {
                        //TimeStamps sort in reverse order so that newer sorts first
                       if (KeyValue.COMPARATOR.compareTimestamps(famKV, kv) <= 0){
                           return null;
                       }
                       else{
                           return checkLengthAndReturn(kv);
                       }
                    }
                    else{
                        return null;
                    }                
            }
            else{
                kv = kvs[pos];
                if (kv.matchingColumn(family, qualifier)) {
                    return checkLengthAndReturn(kv);
                }
                else{
                    return null;
                }
            }
        }
    }
    
    /**
     * This is for the case that the record is of type DeleteColumn, it will have zero length value.
     * this nulls out that case.
     * @param kv
     * @return
     */
    private KeyValue checkLengthAndReturn(KeyValue kv){
        if (kv.getValueLength() > 0){
            return kv;
        }
        else{
            return null;
        }
    }
    
    public int binarySearchForFamily(KeyValue[] kvs, KeyValue searchTerm){
        int pos = Arrays.binarySearch(kvs, searchTerm, KeyValue.COMPARATOR);
        // never will exact match
        if (pos < 0) {
          pos = (pos+1) * -1;
          // pos is now insertion point
        }
        if (pos == kvs.length) {
          return -1; // doesn't exist
        }
        return pos;
    }
    
    public KeyValue createFirstOnFamily(byte[] family, KeyValue kv){
        return new KeyValue(kv.getRow(), family, new byte[]{}, HConstants.LATEST_TIMESTAMP, Type.Maximum);
    }
    
    
    @Override
    public long getWritableSize(){
        return super.getWritableSize() + Bytes.SIZEOF_BOOLEAN;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException{
        super.readFields(input);
        hasDeletes = input.readBoolean();
    }
    
    @Override
    public void write(DataOutput output) throws IOException{
        super.write(output);
        output.writeBoolean(hasDeletes);
    }
    

}
