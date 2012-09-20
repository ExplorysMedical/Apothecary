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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.CachedPeekHFilesScanner;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;
import org.junit.BeforeClass;
import org.junit.Test;

public class MergedStoreFileInputFormatTest{
    public static HBaseAdmin admin;
    public static final String TABLENAME = "patient-3TestTableForMergedStoreFileInputFormatTest";
    public static final byte[] ROWKEY = Bytes.toBytes(MD5Hash.digest("rowkey").toString());
    public static final String COUNTER_FAM = "CountFam";
    public static final String COUNTER_NAME ="numRecords";
    public static final byte[] PFAMILY = Bytes.toBytes("p");
    public static final byte[] DFAMILY = Bytes.toBytes("d");
    public static final byte[] STARTKEY = Bytes.toBytes("000");
    public static final byte[] ENDKEY = Bytes.toBytes("ggg");
    public static HTable table;
    public static HRegionIncommon LOADER = null;
    @BeforeClass
    public static void prepareTable() throws IOException, NoSuchMethodException{
        MergedStoreFileInputFormatTest test = new MergedStoreFileInputFormatTest();
        LOADER = test.getHRegionInCommon();
        test.putInRecordsWithDeleteMask(LOADER);
    }
    
    @Test
    public void testResults() throws IOException, InterruptedException{

        Scan scan = new Scan();
        scan.addFamily(PFAMILY);
        scan.addFamily(DFAMILY);
        MergedStoreFileRecordReader reader = new MergedStoreFileRecordReader(LOADER.region, scan);
        reader.scanner = new CachedPeekHFilesScanner(LOADER.region, scan);
        reader.scanner.initialize();
        
        
        /**
         * Case 1: Sequence of events
         *  1) Write a value to qual1
         *  2) Process a Delete of the entire row
         *  3) Write a value to qual2
         *  
         *  Checking that there is no value at qual1
         *  Checking that there is a value at qual2
         */
        assertTrue(reader.nextKeyValue());
        Result result1 = reader.getCurrentValue();  
        byte[] bytearry = result1.getValue(PFAMILY, Bytes.toBytes("qual1"));
        assertNull(bytearry);
        bytearry = result1.getValue(PFAMILY, Bytes.toBytes("qual2"));
        assertTrue(Bytes.equals(Bytes.toBytes("secondVal"), bytearry));

        
        /**
         * Case 2: Sequence of Events
         *  1) Put a record with value to PFAMILY, put a record with value to DFAMILY
         *  2) Delete PFAMILY
         *  3) Put record in PFAMILY
         * 
         *  Check that record in DFAMILY is there
         *  Check that record after delete is in PFAMILY
         *
         */
        assertTrue(reader.nextKeyValue());
        Result result2 = reader.getCurrentValue();
        
        byte[] pfamnull = result2.getValue(PFAMILY, Bytes.toBytes("qual3"));
        byte[] dfamthere = result2.getValue(DFAMILY, Bytes.toBytes("qual3"));
        
        assertNull(pfamnull);
        assertTrue(Bytes.equals(dfamthere, Bytes.toBytes("otherFamily")));
        byte[] pAfterDelete = result2.getValue(PFAMILY, Bytes.toBytes("qual4"));
        assertTrue(Bytes.equals(pAfterDelete, Bytes.toBytes("secondVal")));

    
        /**
         * Case 3: Sequence of Events
         * 1) Put Value into qual5
         * 2) Put Value into qual6
         * 3) Put Value into qual7
         * 4) Delete Column qual6
         * 5) Delete Column qual7
         * 6) Put Value into qual7
         * 7) Check that qual5 has value
         * 8) Check that qual6 does not
         * 9) Check that qual7 has value
         */
        assertTrue(reader.nextKeyValue());
        Result result3 = reader.getCurrentValue();

        byte[] qual5bytes = result3.getValue(PFAMILY, Bytes.toBytes("qual5"));
        assertTrue(Bytes.equals(qual5bytes, Bytes.toBytes("valueHere")));

        byte[] qual6bytes = result3.getValue(PFAMILY, Bytes.toBytes("qual6"));
        assertNull(qual6bytes);

        
        byte[] qual7bytes = result3.getValue(PFAMILY, Bytes.toBytes("qual7"));
        assertTrue(Bytes.equals(qual7bytes, Bytes.toBytes("qual7val")));
        
        /**
         * Case 4: Put in records so that a DeleteColumn is more recent than a DeleteFamily
         * 1) Put value into 'a'
         * 2) Delete Family
         * 3) Put value into 'b'
         * 4) Put value into 'c'
         * 5) Put value into 'd'
         * 6) DeleteColumn 'c'
         * 7) DeleteColumn 'd'
         * 8) Put value into 'c'
         * 9) check that there are not values in 'a' and 'd'
         * 10) check for the values in 'b' and 'c'
         */

        assertTrue(reader.nextKeyValue());
        Result result4 = reader.getCurrentValue();
        
        byte[] aByte = result4.getValue(PFAMILY, Bytes.toBytes("a"));
        assertNull(aByte);
        
        byte[] bByte = result4.getValue(PFAMILY, Bytes.toBytes("b"));
        byte[] cByte = result4.getValue(PFAMILY, Bytes.toBytes("c"));
        byte[] dByte = result4.getValue(PFAMILY, Bytes.toBytes("d"));
        
        assertNull(aByte);
        assertNull(dByte);
        assertTrue(Bytes.equals(bByte, Bytes.toBytes("b")));
        assertTrue(Bytes.equals(cByte, Bytes.toBytes("c2")));


        /**
         * Case 5: Put in records so that a DeleteFamily is more recent than a DeleteColumn
         * Make sure that the 
         * 1) Delete Column 'p:a'
         * 2) Put value into 'p:a'
         * 3) Put value into 'd:a'
         * 4) Delete Family 'p' 
         * 5) Put value into 'p:b'
         * 6) Check that there is nothing in 'p:a'
         * 7) Check for values in 'd:a' and 'p:b'
         */
        
        assertTrue(reader.nextKeyValue());
        Result result5 = reader.getCurrentValue();
        byte[] pa = result5.getValue(PFAMILY, Bytes.toBytes("a"));
        byte[] pb = result5.getValue(PFAMILY, Bytes.toBytes("b"));
        byte[] da = result5.getValue(DFAMILY, Bytes.toBytes("a"));
        assertNull(pa);
        assertTrue(Bytes.equals(da, Bytes.toBytes("a2")));
        assertTrue(Bytes.equals(pb, Bytes.toBytes("b2")));


    }
    
    

    
    public HRegionIncommon getHRegionInCommon() throws IOException{
        HTableDescriptor descriptor = new HTableDescriptor(TABLENAME);
        descriptor.addFamily(new HColumnDescriptor(PFAMILY));
        descriptor.addFamily(new HColumnDescriptor(DFAMILY));
        //This should include all hexidecimal keys.
        HRegion r = createNewHRegion(descriptor, STARTKEY, ENDKEY);
        HRegionIncommon loader = new HRegionIncommon(r);
        return loader;
    }
    
  
    
    private void addPut(Put put) throws IOException{
        
        LOADER.put(put);
        
    }


    private void putInRecordsWithDeleteMask(HRegionIncommon table) throws IOException {
        //Should Allow for one column to be not deleted, while the other one should be
        
        /**
         * Case 1: Sequence of events
         *  1) Write a value to qual1
         *  2) Process a Delete of the entire row
         *  3) Write a value to qual2
         *  
         *  Checking that there is no value at qual1
         *  Checking that there is a value at qual2
         */
        Put firstVal = new Put(ROWKEY,0);
        firstVal.add(PFAMILY, Bytes.toBytes("qual1"), Bytes.toBytes("valueHere"));
        Delete delete = new Delete(ROWKEY, 1, null);
        Put secondVal = new Put(ROWKEY, 2);
        secondVal.add(PFAMILY, Bytes.toBytes("qual2"), Bytes.toBytes("secondVal"));
  
        addPut(firstVal);
        table.region.delete(delete, null, true);
        addPut(secondVal);
        table.flushcache();

        /**
         * Case 2: Sequence of Events
         *  1) Put a record with value to PFAMILY, put a record with value to DFAMILY
         *  2) Delete PFAMILY
         *  3) Put record in PFAMILY
         * 
         *  Check that record in DFAMILY is there
         *  Check that record after delete is in PFAMILY
         *
         */
        
        firstVal = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x1}), 3);
        firstVal.add(PFAMILY, Bytes.toBytes("qual3"), Bytes.toBytes("valueHere"));
        Put thirdVal = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x1}), 3);
        thirdVal.add(DFAMILY, Bytes.toBytes("qual3"), Bytes.toBytes("otherFamily"));
        delete = new Delete(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x1}), 4, null);
        delete.deleteFamily(PFAMILY, 4);
        secondVal = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x1}), 5);
        secondVal.add(PFAMILY, Bytes.toBytes("qual4"), Bytes.toBytes("secondVal"));

        addPut(firstVal);
        addPut(thirdVal);
        table.region.delete(delete, null, true);
        addPut(secondVal);

        table.flushcache();
        
        /**
         * Case 3: Sequence of Events
         * 1) Put Value into qual5
         * 2) Put Value into qual6
         * 3) Put Value into qual7
         * 4) Delete Column qual6
         * 5) Delete Column qual7
         * 6) Put Value into qual7
         * 7) Check that qual5 has value
         * 8) Check that qual6 does not
         * 9) Check that qual7 has value
         */
        
        firstVal = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x2}), 7);
        firstVal.add(PFAMILY, Bytes.toBytes("qual5"), Bytes.toBytes("valueHere"));
        secondVal = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x2}), 7);
        secondVal.add(PFAMILY, Bytes.toBytes("qual6"), Bytes.toBytes("qual6"));
        thirdVal = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x2}), 7);
        thirdVal.add(PFAMILY, Bytes.toBytes("qual7"), Bytes.toBytes("shouldBeDeleted"));
        delete = new Delete(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x2}), 8, null);
        delete.deleteColumn(PFAMILY, Bytes.toBytes("qual6"), 8);
        Delete delete2 = new Delete(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x2}), 9, null);
        delete2.deleteColumn(PFAMILY, Bytes.toBytes("qual7"), 9);
        Put fourthVal = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x2}), 10);
        fourthVal.add(PFAMILY, Bytes.toBytes("qual7"), Bytes.toBytes("qual7val"));
        addPut(firstVal);
        addPut(secondVal);
        addPut(thirdVal);
        table.region.delete(delete, null, true);
        table.region.delete(delete2, null, true);
        addPut(fourthVal); 
        table.flushcache();
        
        /**
         * Case 4: Put in records so that a DeleteColumn is more recent than a DeleteFamily
         * 1) Put value into 'a'
         * 2) Delete Family
         * 3) Put value into 'b'
         * 4) Put value into 'c'
         * 5) Put value into 'd'
         * 6) DeleteColumn 'c'
         * 7) DeleteColumn 'd'
         * 8) Put value into 'c'
         * 9) check that there are not values in 'a' and 'd'
         * 10) check for the values in 'b' and 'c'
         */
        
        Put aPutOne = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x3}), 0);
        aPutOne.add(PFAMILY, Bytes.toBytes("a"), Bytes.toBytes("a"));
        Delete deleteFam = new Delete(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x3}), 1, null);
        deleteFam.deleteFamily(PFAMILY, 1);
        Put bPutOne = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x3}), 2);
        bPutOne.add(PFAMILY, Bytes.toBytes("b"), Bytes.toBytes("b"));
        Put cPutOne = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x3}), 3);
        cPutOne.add(PFAMILY, Bytes.toBytes("c"), Bytes.toBytes("c"));
        Put dPutOne = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x3}), 4);
        dPutOne.add(PFAMILY, Bytes.toBytes("d"), Bytes.toBytes("d"));
        Delete deleteC = new Delete(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x3}), 5, null);
        deleteC.deleteColumn(PFAMILY, Bytes.toBytes("c"), 5);
        Delete deleteD = new Delete(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x3}), 6, null);
        deleteD.deleteColumn(PFAMILY, Bytes.toBytes("d"), 6);
        Put cPutTwo = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x3}), 7);
        cPutTwo.add(PFAMILY, Bytes.toBytes("c"), Bytes.toBytes("c2"));
        
        addPut(aPutOne);
        table.region.delete(deleteFam, null, true);
        addPut(bPutOne);
        addPut(cPutOne);
        addPut(dPutOne);
        table.region.delete(deleteC, null, true);
        table.region.delete(deleteD, null, true);
        addPut(cPutTwo);
        table.flushcache();
        /**
         * Case 5: Put in records so that a DeleteFamily is more recent than a DeleteColumn
         * Make sure that the 
         * 1) Delete Column 'p:a'
         * 2) Put value into 'p:a'
         * 3) Put value into 'd:a'
         * 4) Delete Family 'p' 
         * 5) Put value into 'p:b'
         * 6) Check that there is nothing in 'p:a'
         * 7) Check for values in 'd:a' and 'p:b'
         */
        
        Delete deleteColA = new Delete(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x4}), 0, null);
        deleteColA.deleteColumn(PFAMILY, Bytes.toBytes("a"));
        Put pFamA = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x4}), 1);
        pFamA.add(PFAMILY, Bytes.toBytes("a"), Bytes.toBytes("a2"));
        Put dFamA = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x4}), 2);
        dFamA.add(DFAMILY, Bytes.toBytes("a"), Bytes.toBytes("a2"));
        deleteFam = new Delete(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x4}), 3, null);
        deleteFam.deleteFamily(PFAMILY, 3);
        Put pFamB = new Put(com.google.common.primitives.Bytes.concat(ROWKEY, new byte[]{0x4}), 4);
        pFamB.add(PFAMILY, Bytes.toBytes("b"), Bytes.toBytes("b2"));
        
        
        table.region.delete(deleteColA, null, true);
        addPut(pFamA);
        addPut(dFamA);
        table.region.delete(deleteFam, null, true);
        addPut(pFamB);
        table.flushcache();
     }
    
    



    /**
     * Test that the behavior of a mix of KeyValue.Type.Put KeyValues and KeyValue.Type.Delete KeyValues correctly masks 
     * the less recent Put KeyValue
     */
    @Test
    public void testDeletes(){
        KeyValue[] kvs = getMaskedKeyValues();
        Arrays.sort(kvs, KeyValue.COMPARATOR);
        Result result = new Result(kvs);
        byte[] qual1 = result.getValue(PFAMILY, Bytes.toBytes("qual1"));
        byte[] qual2 = result.getValue(PFAMILY, Bytes.toBytes("qual2"));

         Assert.assertTrue(Bytes.equals(qual1, new byte[]{}));
         Assert.assertTrue(Bytes.equals(Bytes.toBytes("valueHere"), qual2));
         
        kvs = getMaskedRowKeyValues();
        Arrays.sort(kvs, KeyValue.COMPARATOR);
        result = new Result(kvs);
        qual1 = result.getValue(PFAMILY, Bytes.toBytes("qual1"));
        qual2 = result.getValue(PFAMILY, Bytes.toBytes("qual2"));

         Assert.assertTrue(Bytes.equals(qual1,new byte[]{}));
         Assert.assertTrue(Bytes.equals(Bytes.toBytes("valueHere"), qual2));
       
         
    }
 
    private KeyValue[] getMaskedRowKeyValues() {
        KeyValue kv = new KeyValue(ROWKEY, PFAMILY, Bytes.toBytes("qual1"),System.currentTimeMillis(), KeyValue.Type.Put ,Bytes.toBytes("valueHere"));
        KeyValue[] kvs = new KeyValue[3];
        kvs[0] = kv;

        kv = new KeyValue(ROWKEY, PFAMILY, Bytes.toBytes("qual1") ,System.currentTimeMillis(), KeyValue.Type.Delete);
        kvs[1] = kv;
        
        kv = new KeyValue(ROWKEY, PFAMILY, Bytes.toBytes("qual2"),System.currentTimeMillis(), KeyValue.Type.Put ,Bytes.toBytes("valueHere"));
        kvs[2] = kv;
        

        return kvs;    
        }
    

    private static KeyValue[] getMaskedKeyValues() {
        KeyValue kv = new KeyValue(ROWKEY, PFAMILY, Bytes.toBytes("qual1"),System.currentTimeMillis(), KeyValue.Type.Put ,Bytes.toBytes("valueHere"));
        KeyValue[] kvs = new KeyValue[3];
        kvs[0] = kv;

        
        kv = new KeyValue(ROWKEY, PFAMILY, Bytes.toBytes("qual2"),System.currentTimeMillis(), KeyValue.Type.Put ,Bytes.toBytes("valueHere"));
        kvs[1] = kv;
        
        kv = new KeyValue(ROWKEY, PFAMILY, Bytes.toBytes("qual1"),System.currentTimeMillis(), KeyValue.Type.DeleteColumn);
        kvs[2] = kv;
        return kvs;
    }
    
    /**
     * A class that makes a {@link Incommon} out of a {@link HRegion}
     */
    public static class HRegionIncommon  {
      final HRegion region;

      /**
       * @param HRegion
       */
      public HRegionIncommon(final HRegion HRegion) {
        this.region = HRegion;
      }

      public void put(Put put) throws IOException {
        region.put(put);
      }

      public void delete(Delete delete,  Integer lockid, boolean writeToWAL)
      throws IOException {
        this.region.delete(delete, lockid, writeToWAL);
      }

      public Result get(Get get) throws IOException {
        return region.get(get, null);
      }

      public Result get(Get get, Integer lockid) throws IOException{
        return this.region.get(get, lockid);
      }


      public void flushcache() throws IOException {
        this.region.flushcache();
      }
    }
    
    
    protected static final char FIRST_CHAR = '0';
    protected static final char LAST_CHAR = 'g';


    
    protected HRegion createNewHRegion(HTableDescriptor desc, byte [] startKey,
            byte [] endKey)
        throws IOException {
        Configuration conf = HBaseConfiguration.create();
          FileSystem filesystem = FileSystem.get(conf);
          Path rootdir = filesystem.makeQualified(
              new Path(conf.get(HConstants.HBASE_DIR)));
          filesystem.mkdirs(rootdir);

          return HRegion.createHRegion(new HRegionInfo(desc, startKey, endKey),
              rootdir, conf);
        }

}
