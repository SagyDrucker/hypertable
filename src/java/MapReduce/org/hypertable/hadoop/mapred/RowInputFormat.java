 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)

package org.hypertable.hadoop.mapred;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.hypertable.hadoop.mapreduce.ScanSpec;
import org.hypertable.hadoop.util.Row;
import org.hypertable.thrift.SerializedCellsReader;
import org.hypertable.thrift.SerializedCellsWriter;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.ClientException;

public class RowInputFormat
implements org.apache.hadoop.mapred.InputFormat<BytesWritable, Row>, JobConfigurable {

	final Log LOG = LogFactory.getLog(RowInputFormat.class);


  public static final String NAMESPACE = "hypertable.mapreduce.input.namespace";
  public static final String TABLE = "hypertable.mapreduce.input.table";
  public static final String SCAN_SPEC = "hypertable.mapreduce.input.scan-spec";
  public static final String START_ROW = "hypertable.mapreduce.input.startrow";
  public static final String END_ROW = "hypertable.mapreduce.input.endrow";

  private ThriftClient m_client = null;
  private ScanSpec m_base_spec = null;
  private String m_tablename = null;
  private String m_namespace = null;

  public void configure(JobConf job)
  {
    try {
      if (m_base_spec == null) {
        if(job.get(SCAN_SPEC) == null) {
          System.out.println("Null scan spec in job conf");
          job.set(SCAN_SPEC, (new ScanSpec()).toSerializedText());
        }
        m_base_spec = ScanSpec.serializedTextToScanSpec( job.get(SCAN_SPEC) );
        m_base_spec.setRevs(1);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void set_scan_spec(ScanSpec spec) {
	  m_base_spec = spec;
	  m_base_spec.setRevs(1);
  }
  public ScanSpec getBaseScanSpec(){
	  return m_base_spec;
  }

  public void set_namespace(String namespace) {
    m_namespace = namespace;
  }

  public void set_table_name(String tablename) {
    m_tablename = tablename;
  }
  		 
  
	/**
	 * this class reads rows from hypertable by reading the cells and aggregating cells to make a row.<br>
	 * i've written this class here to test it, and it is implemented in RowInputFormat of the hypertable source code.<br>
	 * in general, this class returns next element in table, by looping the cells in table, and when retrieved cells to make the next row - we return that row
	 * @author benigo
	 *
	 */
	public class HypertableRecordReader
	  implements org.apache.hadoop.mapred.RecordReader<BytesWritable, Row> {
	
	    private ThriftClient m_client = null;
	    private long m_scanner = 0;
	    private long m_ns = 0;
	    private String m_namespace = null;
	    private String m_tablename = null;
	    private ScanSpec m_scan_spec = null;
	    private long m_bytes_read = 0;
	
	    private byte m_serialized_cells[] = null;
	    private Row m_value;
	    private ByteBuffer m_row = null;
	    private BytesWritable m_key = null;
	    private SerializedCellsReader m_reader = new SerializedCellsReader(null);
	
	    private boolean m_eos = false;
	    private String currentKey = "";
	    private int chunkCounter = 1;
	    
	    /**
	     *  Constructor
	     *
	     * @param client Hypertable Thrift client
	     * @param tablename name of table to read from
	     * @param scan_spec scan specification
	     */
	    public HypertableRecordReader(ThriftClient client, String namespace, String tablename, ScanSpec scan_spec)
	        throws IOException {
	
	      m_client = client;
	      m_namespace = namespace;
	      m_tablename = tablename;
	      m_scan_spec = scan_spec;
	
	      try {
	        m_ns = m_client.open_namespace(m_namespace);
	        m_scanner = m_client.open_scanner(m_ns, m_tablename, m_scan_spec);
	        m_row = m_client.next_cells_serialized(m_scanner);	// init buffer for the first time
	        m_reader.reset(m_row);
	        m_reader.next();	// advance reader first time
	      }
	      catch (TTransportException e) {
	        e.printStackTrace();
	        throw new IOException(e.getMessage());
	      }
	      catch (TException e) {
	        e.printStackTrace();
	        throw new IOException(e.getMessage());
	      }
	      catch (ClientException e) {
	        e.printStackTrace();
	        throw new IOException(e.getMessage());
	      }
	    }
	
	    public BytesWritable createKey() {
	        return new BytesWritable();
	    }
	
	    public Row createValue() {
	        return new Row();
	    }
	
	    public void close() {
	      try {
	        m_client.close_scanner(m_scanner);
	        m_client.close_namespace(m_ns);
	      }
	      catch (Exception e) {
	        e.printStackTrace();
	      }
	    }
	
	    public long getPos() throws IOException {
	      return m_bytes_read;
	    }
	    public float getProgress() {
	      // Assume 200M split size
	      if (m_bytes_read >= 200000000)
	        return (float)1.0;
	      return (float)m_bytes_read / (float)200000000.0;
	    }
	    	// BENIGO'S VERSION 
	    @Override
	    public boolean next(BytesWritable key, Row value) throws IOException {	    	
	  	  NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = value.getMap();
	  	  SerializedCellsWriter row = new SerializedCellsWriter(100, true);
	        try {
	        	boolean foundKey = false;
	          while (!m_eos && !foundKey)
	          {
	        	  
	        	  
	        	  boolean doOnce = true;
	        	  boolean hasNextCell = true;
	        	  boolean sameKey = true;
	        	  do	
	        	  {
	        		  if(doOnce)	// assigning currenyKey should happen only once for each do loop - since it doesn't change
	        		  {
	        			  currentKey = new String(m_reader.get_row());  // get next row's key
	        			  key.set(new BytesWritable(m_reader.get_row()));
	        			  doOnce = false;
	        		  }
	        		  row.add(ByteBuffer.wrap(m_reader.get_row()),ByteBuffer.wrap(m_reader.get_column_family()), ByteBuffer.wrap(m_reader.get_column_qualifier()),m_reader.get_timestamp(), ByteBuffer.wrap(m_reader.get_value()));
	        		  
	        		  
	        		  hasNextCell = m_reader.next();
	        		  if(hasNextCell)
	        			  sameKey = currentKey.equals(new String(m_reader.get_row()));
	        		  
	        		  // loop while more cells AND not-end of this row's cells (i.e. same key):
	        	  } while(hasNextCell && sameKey);	// maybe compare keys first??
	        	  if(!sameKey)	// arrived at next key - aggregated all key's cells
	        	  {
	        		  //set the return row - key was already set above
	        		  value.set(new Row(row.array()));
	        		  return true;
	        	  }
	        	  else if(!hasNextCell)	// no more cells, maybe end of chunk
	        	  {
	        		  m_eos = m_reader.eos();
	        		  if(!m_eos){	// get next chunk
	        			  chunkCounter++;
	        			  m_row = m_client.next_cells_serialized(m_scanner);	// init buffer for the first time
	        			  m_reader.reset(m_row);
	        			  if(!m_reader.next()){	// advance first time into new chunk
	        				  return false;
	        			  }else
	        			  {
	        				// if first cell in new chunk, belongs to a different key, that means last chunk ended with whole row, return last row:
	        				  if(!currentKey.equals(new String(m_reader.get_row()))){// different key
	        						value.set(new Row(row.array()));
	    	        		  		return true;
	        				  }	// else (same key in beginning of new chunk) continue normally to add cells to row
	        			  }
	        		  }
	        	  }
	        	  
	          }
	          
	          
	        }catch(Exception e){
	        	e.printStackTrace();
	        }
	        // if row is not empty - return it (reached here means last row in db)
	        if(!row.isEmpty()){
	        	value.set(new Row(row.array()));
	  		  	return true;
	        }else{
	            return false;	// if reached here, no more cells to loop
	        }
	    }
	}
		


  public RecordReader<BytesWritable, Row> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
  throws IOException {
    try {
      TableSplit ts = (TableSplit)split;
      if (m_namespace == null) {
        m_namespace = job.get(NAMESPACE);
      }
      if (m_tablename == null) {
        m_tablename = job.get(TABLE);
      }
      
      ScanSpec scan_spec;
     
      
      scan_spec = ts.createScanSpec();
//    scan_spec = ts.createScanSpec(m_base_spec);
//    ScanSpec scan_spec = m_base_spec;		// m_base_spec is ALREADY like the line above. the method createScanSpec is called up twice

      
      if (m_client == null)
        m_client = ThriftClient.create("localhost", 38080);
      return new HypertableRecordReader(m_client, m_namespace, m_tablename, scan_spec);
    }
    catch (TTransportException e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    catch (TException e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
  }

  public TableSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    long ns=0;
    try {
      if (m_client == null)
        m_client = ThriftClient.create("localhost", 38080);

      String namespace, tablename;
      if (m_namespace == null)
        namespace = job.get(NAMESPACE);
      else
        namespace = m_namespace;
      if (m_tablename == null)
        tablename = job.get(TABLE);
      else
        tablename = m_tablename;
      
      ns = m_client.open_namespace(namespace);
      
      List<org.hypertable.thriftgen.TableSplit> tsplits = m_client.get_table_splits(ns, tablename);
      TableSplit [] splits = new TableSplit[tsplits.size()];
      
      try {
        int pos=0;
        for (final org.hypertable.thriftgen.TableSplit ts : tsplits) {
          byte [] start_row = (ts.start_row == null) ? null : ts.start_row.getBytes("UTF-8");
          byte [] end_row = (ts.end_row == null) ? null : ts.end_row.getBytes("UTF-8");

          TableSplit split = new TableSplit(tablename.getBytes("UTF-8"), start_row, end_row,ts.ip_address);
//          split.setM_scanspec(m_base_spec);
          splits[pos++] = split;
        }
      }
      catch (UnsupportedEncodingException e) {
        e.printStackTrace();
        System.exit(-1);
      }

      return splits;
    }
    catch (TTransportException e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    catch (TException e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    catch (ClientException e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    finally {
      if (ns != 0) {
        try {
          m_client.close_namespace(ns);
        }
        catch (Exception e) {
          e.printStackTrace();
          throw new IOException(e.getMessage());
        }
      }
    }
  }
}


