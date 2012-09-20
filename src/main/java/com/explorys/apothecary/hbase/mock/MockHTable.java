
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
package com.explorys.apothecary.hbase.mock;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * Mock implementation of HTableInterface. 
 * The data is held in an in-memory NavigableMap, 
 * which ensures the ordering of the rows is the same as in HBase.
 * 
 * @author andrew.johnson
 */

public class MockHTable implements HTableInterface {
	/**
	 * This is all the data for a MockHTable instance
	 */
	private NavigableMap<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> tableData
	= new TreeMap<byte[], NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>>(Bytes.BYTES_COMPARATOR);

	/**
	 * Mock implementation does not have a table name
	 * 
	 * @return null
	 */
	@Override
	public byte[] getTableName() {
		return null;
	}

	/**
	 * Mock implementation does not use a Configuration
	 * 
	 * @return null
	 */
	@Override
	public Configuration getConfiguration() {
		return null;
	}

	/**
	 * Mock implementation does not have a table descriptor
	 * 
	 * @return null
	 */
	@Override
	public HTableDescriptor getTableDescriptor() {
		return null;
	}

	/**
	 * Checks if a data exists in the table
	 * 
	 * @param get A Get object describing the parameters for which to search
	 * @return True if the the data exists, false otherwise
	 */
	@Override
	public boolean exists(Get get) throws IOException {
		// If we don't have any column families to search for, only check if the row exists
		if (get.getFamilyMap() == null || get.getFamilyMap().size() == 0) {
			return tableData.containsKey(get.getRow());
		} 
		else {
			byte[] rowKey = get.getRow();
			// If the row key isn't present, then return false immediately
			if (!tableData.containsKey(rowKey)) {
				return false;
			}
			// Now check that each column family we're looking for exists
			for (byte[] family : get.getFamilyMap().keySet()) {
				if (!tableData.get(rowKey).containsKey(family)) {
					return false;
				} 
				else {
					// If the column family exists, check that each qualifier for that family exists
					for (byte[] qualifier : get.getFamilyMap().get(family)) {
						if (!tableData.get(rowKey).get(family).containsKey(qualifier)) {
							return false;
						}
					}
				}
			}
			// At this point everything we're looking for is present
			return true;
		}
	}

	/**
	 * Execute a single get
	 * 
	 * @param get The get to execute
	 * @return A Result object containing the data specified in the get
	 */
	@Override
	public Result get(Get get) throws IOException {
		// If the row key doesn't exist return an empty result
		if (!tableData.containsKey(get.getRow())) {
			return new Result();
		}
		
		byte[] rowKey = get.getRow();
		// This list represents the data requested by the Get and found in the table
		List<KeyValue> keyValueList = new ArrayList<KeyValue>();
		// If no families are specified, return all of the data for the given row key
		if (!get.hasFamilies()) {
			keyValueList = toKeyValue(rowKey, tableData.get(rowKey), get.getMaxVersions());
		} 
		else {
			// Process each requested column family
			for (byte[] family : get.getFamilyMap().keySet()) {
				// If the requested family doesn't exist, move on to the next one
				if (tableData.get(rowKey).get(family) == null) {
					continue;
				}
				NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(family);
				// If qualifiers aren't specified in the get, use all of them that exist in the table
				if (qualifiers == null || qualifiers.isEmpty()) {
					qualifiers = tableData.get(rowKey).get(family).navigableKeySet();
				}
				// Now process each qualifier
				for (byte[] qualifier : qualifiers) {
					// The special empty qualifier
					if (qualifier == null) {
						qualifier = "".getBytes();
					}
					// If the column family:qualifier doesn't exist or doesn't have any data, skip it
					if (!tableData.get(rowKey).containsKey(family)
							|| !tableData.get(rowKey).get(family).containsKey(qualifier)
							|| tableData.get(rowKey).get(family).get(qualifier).isEmpty()) {
						continue;
					}
					// Get the value with the most recent timestamp and add it to list of KeyValue objects
					Entry<Long, byte[]> timestampAndValue = tableData.get(rowKey).get(family).get(qualifier).lastEntry();
					keyValueList.add(new KeyValue(rowKey, family, qualifier, timestampAndValue.getKey(), timestampAndValue.getValue()));
				}
			}
		}
		Filter filter = get.getFilter();
		// If we have a filter, apply it to the data
		if (filter != null) {
			filter.reset();
			List<KeyValue> filteredKeyValueList = new ArrayList<KeyValue>(keyValueList.size());
			for (KeyValue kv : keyValueList) {
				// Stop if we will filter out all remaining KeyValue objects
				if (filter.filterAllRemaining()) {
					break;
				}
				// Skip the current KeyValue if it would get filtered out
				if (filter.filterRowKey(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength())) {
					continue;
				}
				// Add the KeyValue to the filtered list if it should be included
				if (filter.filterKeyValue(kv) == ReturnCode.INCLUDE) {
					filteredKeyValueList.add(kv);
				}
			}
			// If the filter uses filterRow to modify the KeyValue list, apply it
			if (filter.hasFilterRow()) {
				filter.filterRow(filteredKeyValueList);
			}
			keyValueList = filteredKeyValueList;
		}

		// Return a result containing all of the KeyValue objects after filtering
		return new Result(keyValueList);
	}
	
	/**
	 * Execute a list of Gets
	 * 
	 * @param gets A list of Gets to execute
	 * @return An array containing the results from all the given Gets
	 */
	@Override
	public Result[] get(List<Get> gets) throws IOException {
		List<Result> results = new ArrayList<Result>();
		// Execute each get by calling get(Get get) and insert the results into the list
		for (Get g : gets) {
			results.add(get(g));
		}
		// Turn the list into an array to match the return type
		return results.toArray(new Result[results.size()]);
	}

	/**
	 * Gets the value in a given row and column family.  
	 * If the row key does not exist, use the row key immediately before.
	 * 
	 * @param rowKey The row key to use
	 * @param family The column family for which to return data
	 * @return A Result object representing the data in the given row and column family
	 */
	@Override
	public Result getRowOrBefore(byte[] rowKey, byte[] family) throws IOException {
		// If the given row key does not exist, use the row key immediately before it
		if (!tableData.containsKey(rowKey)) {
			rowKey = tableData.lowerKey(rowKey);
		}
		
		// Construct a get for the appropriate row key and column family
		Get g = new Get(rowKey);
		g.addFamily(family);
		
		// Execute this get as normal
		return get(g);
	}

	/**
	 * Gets a scanner for the table from a given Scan
	 * 
	 * @param scan The scan from which to construct the ResultScanner
	 * @return A ResultScanner corresponding to the given Scan object
	 */
	@Override
	public ResultScanner getScanner(Scan scan) throws IOException {
		final List<Result> returnedResults = new ArrayList<Result>();
		byte[] startRow = scan.getStartRow();
		byte[] stopRow = scan.getStopRow();
		Filter filter = scan.getFilter();

		for (byte[] row : tableData.keySet()) {
			// If the row is equal to startRow, emit it
			// This is necessary to ensure that startRow is emitted when startRow=stopRow
			if (startRow != null && startRow.length > 0
					&& Bytes.BYTES_COMPARATOR.compare(startRow, row) != 0) {
				// If the row is before the start row, skip it
				if (startRow != null && startRow.length > 0
						&& Bytes.BYTES_COMPARATOR.compare(startRow, row) > 0) {
					continue;
				}
				// If the row is at or past stopRow, do not emit it and stop iteration
				if (stopRow != null && stopRow.length > 0
						&& Bytes.BYTES_COMPARATOR.compare(stopRow, row) <= 0) {
					break;
				}
			}

			List<KeyValue> keyValueList = null;
			// If the scan does not specify any columns, get all the data for the current row
			if (!scan.hasFamilies()) {
				keyValueList = toKeyValue(row, tableData.get(row), scan.getTimeRange().getMin(), scan.getTimeRange().getMax(), scan.getMaxVersions());
			} 
			else {
				keyValueList = new ArrayList<KeyValue>();
				// Process each column family in the scan
				for (byte[] family : scan.getFamilyMap().keySet()) {
					// If the column family does not exist in the table, move on to the next one
					if (tableData.get(row).get(family) == null) {
						continue;
					}
					NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(family);
					// If the scan does not specify column qualifiers, use all those that exist in the table
					if (qualifiers == null || qualifiers.isEmpty()) {
						qualifiers = tableData.get(row).get(family).navigableKeySet();
					}
					for (byte[] qualifier : qualifiers) {
						// If the column qualifier does not exist in the table, move on to the next one
						if (tableData.get(row).get(family).get(qualifier) == null) {
							continue;
						}
						// Process the cell values in descending order by timestamp
						for (Long timestamp : tableData.get(row).get(family).get(qualifier).descendingKeySet()) {
							// If the timestamp is before the scan's minimum value, skip the current cell
							if (timestamp < scan.getTimeRange().getMin()) {
								continue;
							}
							// If the timestamp is after the scan's maximum value, skip the current cell
							if (timestamp > scan.getTimeRange().getMax()) {
								continue;
							}
							// The current cell is the the appropriate range, so add it to the list
							byte[] cellValue = tableData.get(row).get(family).get(qualifier).get(timestamp);
							keyValueList.add(new KeyValue(row, family, qualifier, timestamp, cellValue));
							// If we've added the maximum number of versions requested, stop processing further cells
							if (keyValueList.size() == scan.getMaxVersions()) {
								break;
							}
						}
					}
				}
			}
			// If the scan includes a filter, apply it
			if (filter != null) {
				filter.reset();
				List<KeyValue> filteredKeyValueList = new ArrayList<KeyValue>(keyValueList.size());
				for (KeyValue kv : keyValueList) {
					// If we would filter all remaining KeyValue objects, stop processing them
					if (filter.filterAllRemaining()) {
						break;
					}
					// Skip this KeyValue object if it should be filtered
					if (filter.filterRowKey(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength())) {
						continue;
					}
					ReturnCode filterResult = filter.filterKeyValue(kv);
					// Add this KeyValue to the filtered list if it should be included
					if (filterResult == ReturnCode.INCLUDE) {
						filteredKeyValueList.add(kv);
					} 
					// If we should move on to the next row, stop processing KeyValues and do so
					else if (filterResult == ReturnCode.NEXT_ROW) {
						break;
					}
				}
				// If the filter uses filterRow to modify the KeyValue list, apply it
				if (filter.hasFilterRow()) {
					filter.filterRow(filteredKeyValueList);
				}
				keyValueList = filteredKeyValueList;
			}
			// If any values were returned from this row, create a Result containing them
			if (!keyValueList.isEmpty()) {
				returnedResults.add(new Result(keyValueList));
			}
		}

		// Methods on ResultScanner are mocked out to use an internal Iterator<Result> from the returnedResults list
		return new ResultScanner() {
			// Use the iterator over the local returnedResults list
			private final Iterator<Result> iterator = returnedResults.iterator();

			public Iterator<Result> iterator() {
				return iterator;
			}

			public Result[] next(int nbRows) throws IOException {
				ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
				for (int i = 0; i < nbRows; i++) {
					Result next = next();
					if (next != null) {
						resultSets.add(next);
					} 
					else {
						break;
					}
				}
				return resultSets.toArray(new Result[resultSets.size()]);
			}

			public Result next() throws IOException {
				try {
					return iterator().next();
				} 
				// If there is no next element, return null
				catch (NoSuchElementException e) {
					return null;
				}
			}

			public void close() { }
		};
	}

	/**
	 * Get a ResultScanner for a given column family
	 * 
	 * @param family The column family to scan
	 * @return A ResultScanner for a scan over the specified family
	 */
	@Override
	public ResultScanner getScanner(byte[] family) throws IOException {
		// Construct a scan over the given family and call getScanner(Scan scan) with it
		Scan scan = new Scan();
		scan.addFamily(family);
		return getScanner(scan);
	}

	/**
	 * Get a ResultScanner for a given column family and qualifier
	 * 
	 * @param family The column family to scan
	 * @param qualifier The column qualifier to scan
	 * @return A ResultScanner for a scan over the specified family and qualifier
	 */
	@Override
	public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
		// Construct a scan over the given column (family:qualifier) and call getScanner(Scan scan) with it
		Scan scan = new Scan();
		scan.addColumn(family, qualifier);
		return getScanner(scan);
	}

	/**
	 * Execute a single Put
	 * 
	 * @param put The put containing the data to insert into the table
	 */
	@Override
	public void put(Put put) throws IOException {
		byte[] rowKey = put.getRow();
		// Either find the existing data for this rowKey or add rowKey to the map
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = findKeyOrAdd(tableData, rowKey,
				new TreeMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>(Bytes.BYTES_COMPARATOR));
		
		// Process each column family in the Put
		for (byte[] family : put.getFamilyMap().keySet()) {
			// Either find the existing data for this family or add the family to the map
			NavigableMap<byte[], NavigableMap<Long, byte[]>> familyData = findKeyOrAdd(rowData, family,
					new TreeMap<byte[], NavigableMap<Long, byte[]>>(Bytes.BYTES_COMPARATOR));
			
			// Process each KeyValue object for the current family
			for (KeyValue keyValue : put.getFamilyMap().get(family)) {
				// Set the timestamp on the KeyValue
				keyValue.updateLatestStamp(Bytes.toBytes(System.currentTimeMillis()));
				byte[] qualifier = keyValue.getQualifier();
				// Either find the existing data for this qualifier or add it to the map
				NavigableMap<Long, byte[]> qualifierData = findKeyOrAdd(familyData, qualifier, new TreeMap<Long, byte[]>());
				// Insert the value from the put with the current timestamp
				qualifierData.put(keyValue.getTimestamp(), keyValue.getValue());
			}
		}
	}

	/**
	 * Execute a list of Puts
	 * 
	 * @param puts A list of puts to execute
	 */
	@Override
	public void put(List<Put> puts) throws IOException {
		// Call put(Put put) for each Put in the list
		for (Put put : puts) {
			put(put);
		}
	}

	/**
	 * Check if a value exists in the table and if so, execute a Put
	 * 
	 * @param rowKey The row key of the row in which to check for the value
	 * @param family The column family in which to check for the value
	 * @param qualifier The column qualifier in which to check for the value
	 * @param value The value for which to check
	 * @param put The put to execute if the value exists
	 * @return True if the value existed and the put was executed, false otherwise
	 */
	@Override
	public boolean checkAndPut(byte[] rowKey, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
		// If the value exists in the specified cell, execute the put
		if (check(rowKey, family, qualifier, value)) {
			put(put);
			return true;
		}
		return false;
	}

	/**
	 * Execute a single Delete
	 * Note that this method does not use tombstones like HBase; it actually removes the data
	 * 
	 * @param delete The Delete to execute
	 */
	@Override
	public void delete(Delete delete) throws IOException {
		byte[] rowKey = delete.getRow();
		
		// If the row key doesn't exist, stop
		if (tableData.get(rowKey) == null) {
			return;
		}
		
		// If there are no families specified in the delete, remove all data associated with the row key
		if (delete.getFamilyMap() == null || delete.getFamilyMap().size() == 0) {
			tableData.remove(rowKey);
			return;
		}
		
		// Process each column family in the delete
		for (byte[] family : delete.getFamilyMap().keySet()) {
			// If the current family does not exist in the table, move on to the next one
			if (tableData.get(rowKey).get(family) == null) {
				continue;
			}
			// If this family doesn't have any data in the table, remove it entirely and move on to the next family
			if (delete.getFamilyMap().get(family).isEmpty()) {
				tableData.get(rowKey).remove(family);
				continue;
			}
			// Remove each KeyValue object specified in the delete from this family
			for (KeyValue kv : delete.getFamilyMap().get(family)) {
				tableData.get(rowKey).get(kv.getFamily()).remove(kv.getQualifier());
			}
			// If after performing the deletes this family has no data in the table, remove it entirely
			if (tableData.get(rowKey).get(family).isEmpty()) {
				tableData.get(rowKey).remove(family);
			}
		}
		
		// If there is no data left for this row key, remove it from the map
		if (tableData.get(rowKey).isEmpty()) {
			tableData.remove(rowKey);
		}
	}

	/**
	 * Execute a list of Deletes
	 * 
	 * @param deletes A list of deletes to execute
	 */
	@Override
	public void delete(List<Delete> deletes) throws IOException {
		// Call delete(Delete delete) for each Delete in the list
		for (Delete delete : deletes) {
			delete(delete);
		}
	}

	/**
	 * Check if a value exists in the table and if so, execute a Delete
	 * 
	 * @param rowKey The row key of the row in which to check for the value
	 * @param family The column family in which to check for the value
	 * @param qualifier The column qualifier in which to check for the value
	 * @param value The value for which to check
	 * @param delete The delete to execute if the value exists
	 * @return True if the value existed and the delete was executed, false otherwise
	 */
	@Override
	public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
		// If the value exists in the specified cell, execute the delete
		if (check(row, family, qualifier, value)) {
			delete(delete);
			return true;
		}
		return false;
	}

	/**
	 * Increment the value in a column
	 * 
	 * @param rowKey The row key of the cell to increment
	 * @param family The column family of the cell to increment
	 * @param qualifier The column qualifier of the cell to increment
	 * @param amount The amount by which to increment the cell value
	 * @return The new value in the cell
	 */
	@Override
	public long incrementColumnValue(byte[] rowKey, byte[] family, byte[] qualifier, long amount) throws IOException {
		return incrementColumnValue(rowKey, family, qualifier, amount, true);
	}

	/**
	 * Increment the value in a column
	 * 
	 * @param rowKey The row key of the cell to increment
	 * @param family The column family of the cell to increment
	 * @param qualifier The column qualifier of the cell to increment
	 * @param amount The amount by which to increment the cell value
	 * @param writeToWAL This parameter is ignored as there is no WAL for MockHTable
	 * @return The new value in the cell
	 */
	@Override
	public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
		// If there is no value in the specified cell, set that cell to the given amount
		if (check(row, family, qualifier, null)) {
			Put put = new Put(row);
			put.add(family, qualifier, Bytes.toBytes(amount));
			put(put);
			return amount;
		}
		
		// If there is a value in the specified cell, add the given amount to that value and put the new value back in the cell
		long newValue = Bytes.toLong(tableData.get(row).get(family).get(qualifier).lastEntry().getValue()) + amount;
		tableData.get(row).get(family).get(qualifier).put(System.currentTimeMillis(), Bytes.toBytes(newValue));
		return newValue;
	}

	/**
	 * Changes are committed immediately to the in-memory map
	 * This is equivalent to auto-flush, so this method always returns true
	 * 
	 * @return True
	 */
	@Override
	public boolean isAutoFlush() {
		return true;
	}

	/**
	 * Mock implementation does not need flushCommits()
	 */
	@Override
	public void flushCommits() throws IOException { }

	/**
	 * Mock implementation does not need close()
	 */
	@Override
	public void close() throws IOException { }

	/**
	 * Mock implementation does not support locking
	 * 
	 * @return null
	 */
	@Override
	public RowLock lockRow(byte[] row) throws IOException {
		return null;
	}

	/**
	 * Mock implementation does not support locking
	 */
	@Override
	public void unlockRow(RowLock rl) throws IOException { }

	/**
	 * Executes a batch of actions
	 * 
	 * @param actions A list of Row actions to execute
	 * @return An array of any results generated by the given actions
	 */
	@Override
	public Object[] batch(List<Row> actions) throws IOException, InterruptedException {
		List<Result> results = new ArrayList<Result>();
		// Each action could be a Delete, a Put, or a Get
		// Call the appropriate method for each action
		for (Row r : actions) {
			if (r instanceof Delete) {
				delete((Delete) r);
			}
			else if (r instanceof Put) {
				put((Put) r);
			}
			else if (r instanceof Get) {
				// Only a Get will generate results
				results.add(get((Get) r));
			}
		}
		return results.toArray();
	}

	/**
	 * Executes a batch of actions and puts the results into an existing array
	 * 
	 * @param actions A list of Row actions to execute
	 * @param results The array which will contain any results generated by the given actions
	 */
	@Override
	public void batch(List<Row> actions, Object[] results) throws IOException, InterruptedException {
		// Call batch(List<Row> actions) with the specified actions and put the results into the given array
		results = batch(actions);
	}

	/**
	 * Executes an Increment operation
	 * 
	 * @param increment The increment operation to execute
	 * @return Result a result containing the new values after the increment was performed
	 */
	@Override
	public Result increment(Increment increment) throws IOException {
		List<KeyValue> keyValueList = new ArrayList<KeyValue>();
		Map<byte[], NavigableMap<byte[], Long>> familyMap = increment.getFamilyMap();
		// Process each column family in the Increment
		for (Entry<byte[], NavigableMap<byte[], Long>> ef : familyMap.entrySet()) {
			byte[] family = ef.getKey();
			// This maps between the column qualifier and the amount the value in that column should be incremented
			NavigableMap<byte[], Long> colToIncrementMap = ef.getValue();
			for (Entry<byte[], Long> eq : colToIncrementMap.entrySet()) {
				// Increment the current column and add the new value to the Result to be returned
				incrementColumnValue(increment.getRow(), family, eq.getKey(), eq.getValue());
				keyValueList.add(new KeyValue(increment.getRow(), family, eq.getKey(), Bytes.toBytes(eq.getValue())));
			}
		}
		return new Result(keyValueList);
	}

	/**
	 * Converts some data into a list of KeyValues
	 * 
	 * @param rowKey row key of the data
	 * @param rowdata data to convert to KeyValues
	 * @param maxVersions number of versions to return
	 * @return List of KeyValues
	 */
	private static List<KeyValue> toKeyValue(byte[] rowKey, 
			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData, int maxVersions) {
		return toKeyValue(rowKey, rowData, 0, Long.MAX_VALUE, maxVersions);
	}

	/**
	 * Converts some data into a list of KeyValues within a timestamp range
	 * 
	 * @param rowKey row key of the data
	 * @param rowData data to convert to KeyValues
	 * @param timestampStart smallest version to convert
	 * @param timestampEnd largest version to convert
	 * @param maxVersions number of versions to return
	 * @return List of KeyValues
	 */
	private static List<KeyValue> toKeyValue( byte[] rowKey,
			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData,
			long timestampStart, long timestampEnd, int maxVersions) {
		List<KeyValue> keyValueList = new ArrayList<KeyValue>();
		// Process each column family in the data
		for (byte[] family : rowData.keySet()) {
			// Process each qualifier in the data
			for (byte[] qualifier : rowData.get(family).keySet()) {
				int versionsAdded = 0;
				// Process each (timestamp, value) pair in the current column
				for (Entry<Long, byte[]> timestampValueMap : rowData.get(family).get(qualifier).descendingMap().entrySet()) {
					// If we have added the correct number of versions, stop processing
					if (versionsAdded++ == maxVersions) {
						break;
					}
					Long timestamp = timestampValueMap.getKey();
					// Skip to the next value if the timestamp is not in range
					if (timestamp < timestampStart) {
						continue;
					}
					if (timestamp > timestampEnd) {
						continue;
					}
					byte[] value = timestampValueMap.getValue();
					// Create a KeyValue object from the current cell data
					keyValueList.add(new KeyValue(rowKey, family, qualifier, timestamp, value));
				}
			}
		}
		return keyValueList;
	}

	/**
	 * Checks if the value with given details exists in database, or is
	 * non-existent in the case of value being null
	 * 
	 * @param row The row key of the data
	 * @param family The column family
	 * @param qualifier The column qualifier
	 * @param value The value for which to check
	 * @return true if value is not null and exists in db, or value is null and
	 *         does not exist in db, false otherwise
	 */
	private boolean check(byte[] row, byte[] family, byte[] qualifier, byte[] value) {
		// If the value is null, we want to check that there is no value in the given cell
		if (value == null || value.length == 0) {
			return !tableData.containsKey(row) || !tableData.get(row).containsKey(family)
					|| !tableData.get(row).get(family).containsKey(qualifier);
		}
		// If the value is not null, we want to check that the value is present as the most recent
		// entry in the given cell
		else {
			return tableData.containsKey(row)
					&& tableData.get(row).containsKey(family)
					&& tableData.get(row).get(family).containsKey(qualifier)
					&& !tableData.get(row).get(family).get(qualifier).isEmpty()
					&& Arrays.equals(tableData.get(row).get(family).get(qualifier)
							.lastEntry().getValue(), value);
		}
	}

	/**
	 * Finds a key in a map. If key is not found, newObject is added to map and returned
	 * 
	 * @param map Map from which to find value
	 * @param key The key for which to look
	 * @param newObject If the key is not found, add it to the map with this value
	 * @return found value or newObject if not found
	 */
	private <K, V> V findKeyOrAdd(NavigableMap<K, V> map, K key, V newObject) {
		V data = map.get(key);
		if (data == null) {
			data = newObject;
			map.put(key, data);
		}
		return data;
	}
}
