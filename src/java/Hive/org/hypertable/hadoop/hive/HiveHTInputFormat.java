/**
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

package org.hypertable.hadoop.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.hypertable.hadoop.mapred.RowInputFormat;
import org.hypertable.hadoop.mapred.RowInputFormat.HypertableRecordReader;
import org.hypertable.hadoop.mapred.TableSplit;
import org.hypertable.hadoop.mapreduce.ScanSpec;
import org.hypertable.hadoop.util.Row;
import org.hypertable.thriftgen.RowInterval;

/**
 * HiveHTInputFormat implements InputFormat for Hypertable storage handler
 * tables, decorating an underlying Hypertable RowInputFormat with extra Hive logic
 * such as column pruning.
 */
public class HiveHTInputFormat<K extends BytesWritable, V extends Row>
    implements InputFormat<K, V>, JobConfigurable {

  static final Log LOG = LogFactory.getLog(HiveHTInputFormat.class);

  private RowInputFormat htRowInputFormat;
  private String tablename;
  private String namespace;
  private ScanSpec scanspec;
  
  private String originalQuery = "";
  private RowInterval rowInterval;
  
  

  public HiveHTInputFormat() {
    scanspec = new ScanSpec();
    htRowInputFormat = new RowInputFormat();
  }

  @Override
  public RecordReader<K, V> getRecordReader(
    InputSplit split, JobConf job,
    Reporter reporter) throws IOException {
    HiveHTSplit htSplit = (HiveHTSplit)split;
    LOG.info("getRecReader. split is: "+split);
    LOG.info("row interval is: "+htSplit.getSplit().getM_scanspec().getRow_intervals());
    LOG.info("row regex is: "+htSplit.getSplit().getM_scanspec().getRow_regexp());
    
    if (namespace == null) {
      namespace = job.get(HTSerDe.HT_NAMESPACE);
    }
    if (tablename == null) {
      tablename = job.get(HTSerDe.HT_TABLE_NAME);
    }
    String htColumnsMapping = job.get(HTSerDe.HT_COL_MAPPING);
    List<String> htColumnFamilies = new ArrayList<String>();
    List<String> htColumnQualifiers = new ArrayList<String>();
    List<byte []> htColumnFamiliesBytes = new ArrayList<byte []>();
    List<byte []> htColumnQualifiersBytes = new ArrayList<byte []>();

    int iKey;
    try {
      iKey = HTSerDe.parseColumnMapping(htColumnsMapping, htColumnFamilies,
          htColumnFamiliesBytes, htColumnQualifiers, htColumnQualifiersBytes);
    } catch (SerDeException se) {
      throw new IOException(se);
    }
    List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(job);

    if (htColumnFamilies.size() < readColIDs.size()) {
      throw new IOException("Cannot read more columns than the given table contains.");
    }

    boolean addAll = (readColIDs.size() == 0);
    boolean keys_only = true;
    
      scanspec = htSplit.getSplit().getM_scanspec();

    scanspec.unsetColumns();
    if (!addAll) {
      for (int ii : readColIDs) {
        if (ii == iKey) {
          continue;
        }

        if (htColumnQualifiers.get(ii) == null) {
          scanspec.addToColumns(htColumnFamilies.get(ii));
        } else {
          String column = htColumnFamilies.get(ii)+ ":" + htColumnQualifiers.get(ii);
          scanspec.addToColumns(column);
        }
        keys_only = false;
      }
    } else {
      for (int ii=0; ii<htColumnFamilies.size(); ii++) {
        if (ii == iKey)
          continue;
        if (htColumnQualifiers.get(ii) == null) {
          scanspec.addToColumns(htColumnFamilies.get(ii));
        } else {
          String column = htColumnFamilies.get(ii)+ ":" + htColumnQualifiers.get(ii);
          scanspec.addToColumns(column);
        }
      }
      keys_only = false;
    }

    // The Hypertable table's row key maps to a Hive table column.
    // In the corner case when only the row key column is selected in Hive,
    // ask HT to return keys only
    if (keys_only)
      scanspec.setKeys_only(true);

    scanspec.setRevs(1);

    ScanSpec spec = htSplit.getSplit().createScanSpec();

    htRowInputFormat.set_scan_spec(spec);
    htRowInputFormat.set_namespace(namespace);
    htRowInputFormat.set_table_name(tablename);
    
//    HypertableRecordReader x = (HypertableRecordReader) htRowInputFormat.getRecordReader(htSplit.getSplit(), job, reporter);
    
    return (RecordReader<K, V>)
      htRowInputFormat.getRecordReader(htSplit.getSplit(), job, reporter);
  }
  
  
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
//	job.set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat");
	
//	Iterator<Entry<String, String>> jobit = job.iterator();
//	  while(jobit.hasNext())
//	  {
//		  Entry<String, String> ent = jobit.next();
//		  System.out.println(ent.getKey()+"\t"+ent.getValue());
//	  }
//	  System.exit(0);
	
	originalQuery = job.get("hive.query.string");
	if(originalQuery==null && job.get("hypertable.mapreduce.input.scan-spec").contains("AAAAA")){ // select * - no condition query - no limit
		originalQuery = "select * from table";
	}
	else
		originalQuery = originalQuery.toLowerCase();
	
	System.out.println("original query: " +originalQuery);

	
    Path [] tableNames = FileInputFormat.getInputPaths(job);
    String htNamespace = job.get(HTSerDe.HT_NAMESPACE);
    String htTableName = job.get(HTSerDe.HT_TABLE_NAME);
    htRowInputFormat.set_namespace(htNamespace);
    htRowInputFormat.set_table_name(htTableName);

    String htColumnsMapping = job.get(HTSerDe.HT_COL_MAPPING);
    if (htColumnsMapping == null) {
      throw new IOException("hypertable.columns.mapping required for Hypertable Table.");
    }

    List<String> htColumnFamilies = new ArrayList<String>();
    List<String> htColumnQualifiers = new ArrayList<String>();    
    List<byte []> htColumnFamiliesBytes = new ArrayList<byte []>();
    List<byte []> htColumnQualifiersBytes = new ArrayList<byte []>();
    
    List<String> hiveColumnNames = new ArrayList<String>();
    
    int iKey;
    String[] columns;
    
    try {
      iKey = HTSerDeImproved.parseColumnMapping(htColumnsMapping, htColumnFamilies,
          htColumnFamiliesBytes, htColumnQualifiers, htColumnQualifiersBytes);
      
      columns = job.getStrings(org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS);
      if(columns!=null)
    	  hiveColumnNames.addAll(Arrays.asList(columns));
      
    } catch (SerDeException se) {
      throw new IOException(se);
    }
    List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(job);
    
    if (htColumnFamilies.size() < readColIDs.size()) {
      throw new IOException("Cannot read more columns than the given table contains.");
    }

    boolean addAll = (readColIDs.size() == 0);
    boolean keys_only = true;
    scanspec.unsetColumns();
    scanspec.setRow_regexpIsSet(false);		// reset row regex
    scanspec.setRow_intervalsIsSet(false);  // reset row intervals
    if (!addAll) {
      for (int ii : readColIDs) {
        if (ii == iKey) {
          continue;
        }

        if (htColumnQualifiers.get(ii) == null) {
          scanspec.addToColumns(htColumnFamilies.get(ii));
        } else {
          String column = htColumnFamilies.get(ii)+ ":" + htColumnQualifiers.get(ii);
          scanspec.addToColumns(column);
        }
        keys_only = false;
      }
    } else {
      for (int ii=0; ii<htColumnFamilies.size(); ii++) {
        if (ii == iKey)
          continue;
        if (htColumnQualifiers.get(ii) == null) {
          scanspec.addToColumns(htColumnFamilies.get(ii));
        } else {
          String column = htColumnFamilies.get(ii)+ ":" + htColumnQualifiers.get(ii);
          scanspec.addToColumns(column);
        }
      }
      keys_only = false;
    }
    // The Hypertable table's row key maps to a Hive table column.
    // In the corner case when only the row key column is selected in Hive,
    // ask HT to return keys only
    if (keys_only)
      scanspec.setKeys_only(true);
    scanspec.setRevs(1);
    
    
    // rowInterval = parseQuery(hiveColumnNames.get(iKey));  // there is a problem here!!
    		/* the problem is that sometimes hiveColumnNames is empty, in queries where no columns needed. e.g. select *)*/
    //my solution:
    rowInterval = (columns!=null) ? parseQuery(hiveColumnNames.get(iKey)) : null;
    
	if(rowInterval!=null)
	{
		scanspec.addToRow_intervals(rowInterval);
	}
	
    htRowInputFormat.set_scan_spec(scanspec);

    int num_splits=0;
    TableSplit [] splits = htRowInputFormat.getSplits(job, num_splits);
    
    
    if(rowInterval!=null)
    {
    	splits = removeSplitsByRowInterval(splits);
    
    }
    
    InputSplit [] results = new InputSplit[splits.length];
    for (int ii=0; ii< splits.length; ii++) {
    	splits[ii].setM_scanspec(scanspec);
      results[ii] = new HiveHTSplit( splits[ii], htColumnsMapping, tableNames[0]);
    }
    
    
    return results;
  }
  
  /**
	 * this function removes from the split array, the splits of the database which do NOT match with the rowInterval created by query
	 * @param splits all the different splits of the database
	 * @return
	 */
private TableSplit[] removeSplitsByRowInterval(TableSplit[] splits) {
	List<InputSplit> splitsAsList = new ArrayList<InputSplit>();
	List<InputSplit> splitsToRemove = new ArrayList<InputSplit>();
	int removedCount = 0;
//	System.out.println("rowInterval: "+rowInterval);
	for(TableSplit split : splits)	// loop on all splits
	{
		try 
		{
			splitsAsList.add(split);	// add all splits to a list
		
			String mStartRow =	(split.getStartRow()==null) ? "" : new String(split.getStartRow(), "UTF-8");
			String mEndRow =  	(split.getEndRow()==null) 	 ? "" : new String(split.getEndRow(),"UTF-8");
			
			if(rowInterval.getEnd_row()!=null && rowInterval.getEnd_row().compareTo(mStartRow)<0){	
				splitsToRemove.add(split);
				removedCount++;
			}
			else if((!mEndRow.isEmpty()) && rowInterval.getStart_row()!=null && rowInterval.getStart_row().compareTo(mEndRow)>0){
				splitsToRemove.add(split);
				removedCount++;
			}
		
		}catch(Exception e){
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}
	System.out.println("Filtered "+removedCount+"/"+splits.length+" splits using pushdown condition");
	splitsAsList.removeAll(splitsToRemove);
//	InputSplit[] cleanedArray = (InputSplit[]) splitsAsList.toArray();
	TableSplit[] cleanedArray = splitsAsList.toArray(new TableSplit[0]);

	return cleanedArray;
}
 
private void addCondToInterval(String expressionString, RowInterval rowInterval,String key) {
	  //split by predicator:
	  Pattern predPattern = Pattern.compile("(<=?|>=?|=|r?like)");
	  Matcher m = predPattern.matcher(expressionString);
	  m.find();
	  int indexOfPred = m.start();
	  int lastIndexOfPred = m.end();
	  
	  String pred = expressionString.substring(indexOfPred, lastIndexOfPred).trim();
	  String val = expressionString.substring(lastIndexOfPred).trim();
	  
	  // clean val from single quotes. '88' --> 88
	  val = val.replaceAll("'", "");
	  
//	  // clean val if pred is not a java regex expression (only rlike. like is sql-regex and should be cleaned)
//	  if(!pred.contains("rlike"))	
//		  val = val.replaceAll("[\\W]", "");
//	  else // remove only '," (quotes) from input when pred=rlike
//		  val = val.replaceAll("'|\"", "");

	  if(key.equals(val))	// reverse order. e.g. "3 < key" rather than "key > 3" --> call reverse order 
	  {	
		  addCondToInterval(val + " " + reversePredicate(pred) + " " + key, rowInterval,key);
		  return;
	  }
	  

	  if(pred.equals(">")){
			  rowInterval.setStart_row(val);
			  rowInterval.setStart_inclusive(false);
			  System.out.println("added: row > " + val);
	  }else if(pred.equals(">=")){
			  rowInterval.setStart_row(val);
			  rowInterval.setStart_inclusive(true);
			  System.out.println("added: row >= " + val);
	  }else if(pred.equals("<")){
			  rowInterval.setEnd_row(val);
			  rowInterval.setEnd_inclusive(false);
			  System.out.println("added: row < " + val);
	  }else if(pred.equals("<=")){
			  rowInterval.setEnd_row(val);
			  rowInterval.setEnd_inclusive(true);
			  System.out.println("added: row <= " + val);
	  }else if(pred.equals("=")){
		  	rowInterval.setStart_row(val);
		  	rowInterval.setStart_inclusive(true);
		  	rowInterval.setEnd_row(val);
		  	rowInterval.setEnd_inclusive(true);
		  	System.out.println("added: row = " + val);
	  }else if(pred.contains("like")){	// i used contains to include both "like" and "rlike" cases...
		  	scanspec.setRow_regexp(val);
		  	scanspec.setRow_regexpIsSet(true);
		  	System.out.println("added: regex: " + val);
	  }
	  else
		  	System.out.println("didn't add any cond. expression: " + expressionString);
  }

  private String reversePredicate(String pred) {
	if("<".equals(pred))
		return ">";
	else if(">".equals(pred))
		return "<";
	else if(">=".equals(pred))
		return "<=";
	else if(">=".equals(pred))
		return "<=";
	else
		return pred;
  }
  
  /**
   * this function takes the hql query, parse it, and add its conditions to rowInterval.<br>
   * NOTE: conditions it knows how to handle are only <,<=,>,>=,=,like,rlike
   * @return rowInterval - the interval of rows to be scanned - according to conditions in query
   */
  
  private RowInterval parseQuery(String keyCol) {
	  System.out.println("parsing query...");
	  RowInterval rowInterval = new RowInterval();
	  Pattern conditionPattern = Pattern.compile(keyCol+"\\s?(<=?|>=?|=|r?like)\\s?[^\\s$]+");
	  boolean hasInterval = false;
	try
	{
			Matcher matcherCond = conditionPattern.matcher(originalQuery);
			while(matcherCond.find())
			{
				String condition = matcherCond.group();
				System.out.println("evaluating condition: " + condition);
				addCondToInterval(condition, rowInterval,keyCol);				
				hasInterval = true;
			}
			
	} catch (Exception e) {
		e.printStackTrace();
	}
	if(hasInterval)
		return rowInterval;
	return null;
	
  }
  
  @Override
  public void configure(JobConf job) {
    htRowInputFormat.configure(job);
  }
}