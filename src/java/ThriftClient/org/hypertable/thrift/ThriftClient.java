/**
 * Copyright (C) 2008  Luke Lu (Zvents, Inc.)
 *
 * This file is distributed under the Apache Software License
 * (http://www.apache.org/licenses/)
 */

package org.hypertable.thrift;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NavigableMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.hypertable.hadoop.util.Row;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.HqlService;
import org.hypertable.thriftgen.ScanSpec;

public class ThriftClient extends HqlService.Client {
  public ThriftClient(TProtocol protocol) { super(protocol); }

  
  
  // Java only allow super as the first statement of constructor.  It doesn't
  // support multiple inheritance to use the base-from-member idiom either. So,
  // we're resorting to a static factory method here.
  public static ThriftClient
  create(String host, int port, int timeout_ms, boolean do_open)
      throws TTransportException, TException {
    TFramedTransport transport = new TFramedTransport(new TSocket(host, port, timeout_ms));
    
    ThriftClient client = new ThriftClient(new TBinaryProtocol(transport));
    client.transport = transport;

    if (do_open)
      client.open();

    return client;
  }

  // Java doesn't support default argument values, which makes things
  // unnecessarily verbose here
  public static ThriftClient create(String host, int port)
      throws TTransportException, TException {
    return create(host, port, 1600000, true);
  }

  public static ThriftClient create(String host, int port,String namespace,String tablename, ScanSpec scan_spec)
  throws TTransportException, TException {
	  return create(host, port, 1600000, true);
  }
  
  public void open() throws TTransportException, TException {
    transport.open();
    do_close = true;
  }

  public void close() {
    if (do_close) {
      transport.close();
      do_close = false;
    }
  }

  protected TFramedTransport transport;
  private boolean do_close = false;
  /**
	 * this variable instance, is the feature i added in order to implement: "get_row_serialized_fromBufferedCells"<br>
	 * what it does it returning the next row in hypertable.<br>
	 * it's implementation is getting bulks of cells, looping them, and separating the cells by the different rows.<br>
	 * it's an object with 1 method 
	 */
	private RowsRecordReaderBuffered hypertableRecordReaderBuffered = null;
	
	public RowsRecordReaderBuffered getRowsRecordReaderBuffered(long scanner){
		try {
			hypertableRecordReaderBuffered = new RowsRecordReaderBuffered(this, scanner);
		} catch (IOException e) {e.printStackTrace();}
		
		return hypertableRecordReaderBuffered;
	}
	
	/*
	public ByteBuffer get_row_serialized_fromBufferedCells(long scanner){
		if(scanner!=hypertableRecordReaderBuffered.m_scanner)	// i'm not sure what to do with the parameter scanner, coz object holds a scanner long
		{
			System.out.println("wrong scanner provided. object hypertableRecordReaderBuffered got a different scanner already");
			System.out.println("is this a bug??? if so fix it... exiting...");
			System.exit(0);
		}
		BytesWritable key = new BytesWritable();
		Row value = new Row();
		ByteBuffer valueAsByteBuffer = ByteBuffer.allocate(1024);
		ByteBuffer flippedValueAsByteBuffer = ByteBuffer.allocate(1024);
		try {
			hypertableRecordReaderBuffered.next(key, value, valueAsByteBuffer);
			flippedValueAsByteBuffer = (ByteBuffer) valueAsByteBuffer.flip();
		} catch (IOException e) {e.printStackTrace();}
		
		return flippedValueAsByteBuffer;
	}
	*/
	
	/**
	 * this class reads rows from hypertable by reading the cells and aggregating cells to make a row.<br>
	 * in general, this class has 1 function: next() which returns next element in table, by looping the cells in table, and when retrieved cells to make the next row - we return that row
	 * @author benigo
	 *
	 */
	public class RowsRecordReaderBuffered
		implements org.apache.hadoop.mapred.RecordReader<BytesWritable, Row>{

	    private ThriftClient m_client = null;
	    private long m_scanner = 0;
	    private ScanSpec m_scan_spec = null;
	    private long m_bytes_read = 0;

	    private byte m_serialized_cells[] = null;
	    private Row m_value;
	    private ByteBuffer m_row = null;
	    private BytesWritable m_key = null;
	    private SerializedCellsReader m_reader = new SerializedCellsReader(null);

	    private boolean m_eos = false;
	    private String lastKey = "";
	    private String currentKey = "";
	    private int chunkCounter = 1;
	    
	    /**
	     *  Constructor
	     *
	     * @param client Hypertable Thrift client
	     * @param tablename name of table to read from
	     * @param scan_spec scan specification
	     */
	    public RowsRecordReaderBuffered(ThriftClient client, long mScanner)
	        throws IOException {

	      m_client = client;

	      try {
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
	    public boolean next(BytesWritable key, Row value,ByteBuffer valueAsByteBuffer) throws IOException {
	  	  NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = value.getMap();
	  	  SerializedCellsWriter row = new SerializedCellsWriter(1024, true);
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
//	        		  byte[] valueOfCell = m_reader.get_value();
//	        		  System.out.println(new String (valueOfCell));
	        		  
	        		  
	        		  hasNextCell = m_reader.next();
	        		  if(hasNextCell)
	        			  sameKey = currentKey.equals(new String(m_reader.get_row()));
	        		  
	        		  // loop while more cells AND not-end of this row's cells (i.e. same key):
	        	  } while(hasNextCell && sameKey);	// maybe compare keys first??
	        	  if(!sameKey)	// arrived at next key - aggregated all key's cells
	        	  {
	        		  //set the return row - key was already set above
	        		  value.set(new Row(row.array()));
	        		  valueAsByteBuffer.put(row.buffer());
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
	        						valueAsByteBuffer.put(row.buffer());
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
	        	valueAsByteBuffer.put(row.buffer());
      		  	return true;
	        }else{
	            return false;	// if reached here, no more cells to loop
	        }
	    }

		@Override
		public boolean next(BytesWritable arg0, Row arg1)throws IOException {
			ByteBuffer valueAsByteBuffer = ByteBuffer.allocate(1024);
			return next(arg0, arg1, valueAsByteBuffer);
		}
	}
	
}
