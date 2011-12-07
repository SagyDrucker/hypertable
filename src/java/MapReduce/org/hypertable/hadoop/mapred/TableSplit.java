 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)

package org.hypertable.hadoop.mapred;

import java.io.DataInput;

/**
 * A table split corresponds to a key range (low, high). All references to row
 * below refer to the key of the row.<br>
 */
public class TableSplit
implements InputSplit,Comparable<TableSplit> {

  protected byte [] m_tablename;
  protected byte [] m_startrow;
  protected byte [] m_endrow;
  protected String m_hostname;
  protected ScanSpec m_scanspec;
  final Log LOG = LogFactory.getLog(TableSplit.class);

  /** Default constructor. */
  public TableSplit() {
      this(new byte [0], new byte [0], new byte [0], "");
  }

  /**
   * Constructs a new instance while assigning all variables.
   *
   * @param tableName  The name of the current table.
   * @param startRow  The start row of the split.
   * @param endRow  The end row of the split.
   * @param hostname  The hostname of the range.
   */
  public TableSplit(byte [] tableName, byte [] startRow, byte [] endRow,
                    final String hostname) {
    this.m_tablename = tableName;
    this.m_startrow = startRow;
    this.m_endrow = endRow;
    this.m_hostname = hostname;
    // Check for END_ROW marker
    if (endRow != null && endRow.length == 2 &&
        endRow[0] == (byte)0xff && endRow[1] == (byte)0xff) {
      this.m_endrow = null;
    }
  }

  /**
   * Returns the table name.
   *
   * @return The table name.
   */
  public byte [] getTableName() {
    return m_tablename;
  }

  /**
   * Returns the start row.
   *
   * @return The start row.
   */
  public byte [] getStartRow() {
    return m_startrow;
  }

  /**
   * Returns the end row.
   *
   * @return The end row.
   */
  public byte [] getEndRow() {
    return m_endrow;
  }

  /**
   * Returns the range location.
   *
   * @return The range's location.
   */
  public String getRangeLocation() {
    return m_hostname;
  }

  /**
   * Returns the range's location as an array.
   *
   * @return The array containing the range location.
   * @see org.apache.hadoop.mapreduce.InputSplit#getLocations()
   */
  public String[] getLocations() {
    int period_offset = m_hostname.indexOf('.');
    if (period_offset == -1)
      return new String[] {m_hostname};
    return new String[] {m_hostname, m_hostname.substring(0, period_offset)};
  }

  /**
   * Returns the length of the split.
   *
   * @return The length of the split.
   * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
   */
  public long getLength() {
    // Not clear how to obtain this... seems to be used only for sorting splits
    return 0;
  }


  /**
   * returns an instance of scanspec copied from the local field ScanSpec<br>
   * unlike the original createScanSpec(ScanSpec base_spec) , this function does not get
   * a scan spec as parameter, since we are using the local field m_scanspec which was previously assigned.
   *
   * @author Sagy Drucker (sagysrael@hotmail.com)
   * @param scan_spec The base ScanSpec to start with
   * @return a new scan_spec object with a row interval matching this split
   */
  
  public ScanSpec createScanSpec()
  {
//	  LOG.info("creating scanspec instance");
//	  LOG.info("m_scanspec row regex: " + m_scanspec.getRow_regexp());
//	  LOG.info("m_scanspec value intervals: " + m_scanspec.getRow_intervals());
	  
	    ScanSpec scan_spec = new ScanSpec(m_scanspec);
	    RowInterval interval = new RowInterval();

	    scan_spec.unsetRow_intervals();
	    
	    	try 
	    	{
	    	if(m_startrow != null && m_startrow.length > 0) {
					interval.setStart_row(new String(m_startrow, "UTF-8"));
				interval.setStart_rowIsSet(true);
				interval.setStart_inclusive(false);
				interval.setStart_inclusiveIsSet(true);
			}
			
			if(m_endrow != null && m_endrow.length > 0) {
				interval.setEnd_row(new String(m_endrow,"UTF-8"));
				interval.setEnd_rowIsSet(true);
				interval.setEnd_inclusive(true);
				interval.setEnd_inclusiveIsSet(true);
			}
			
			if (m_scanspec.isSetRow_intervals()) {
				for (RowInterval ri : m_scanspec.getRow_intervals()) {
					if (ri.isSetStart_row()) {
						if (m_startrow == null ||
								ri.getStart_row().compareTo(new String(m_startrow, "UTF-8")) > 0) {
							interval.setStart_row(ri.getStart_row());
							interval.setStart_rowIsSet(true);
							interval.setStart_inclusive( ri.isStart_inclusive() );
							interval.setStart_inclusiveIsSet(true);
						}
					}
					if (ri.isSetEnd_row()) {
						if ( m_endrow == null || m_endrow.length == 0 ||
								ri.getEnd_row().compareTo(new String(m_endrow,"UTF-8")) < 0)  {
							interval.setEnd_row(ri.getEnd_row());
							interval.setEnd_rowIsSet(true);
							interval.setEnd_inclusive( ri.isEnd_inclusive() );
							interval.setEnd_inclusiveIsSet(true);
						}
					}
					// Only allowing a single row interval
					break;
				}
			}
	    } catch (UnsupportedEncodingException e) {
    		e.printStackTrace();
    	}
	    
	    if(interval.isSetStart_row() || interval.isSetEnd_row()) {
	    	scan_spec.addToRow_intervals(interval);
	    	scan_spec.setRow_intervalsIsSet(true);
	    }
	    
	    if(m_scanspec.isSetRow_regexp()){
	    	scan_spec.setRow_regexpIsSet(true);
	    	scan_spec.setRow_regexp(m_scanspec.getRow_regexp());
	    }
	    	
	    
	    return scan_spec;
	  }
  
  /**
   * Updates the ScanSpec by setting the row interval to match this split<br>
   * This overriding method, also checks that base_spec's intervals synchronized with scnaspec m_start/end-row, by which the table was split.<br>
   * this prevents such as if splits: [1-3,4-8,9-10] is created, and interval is 2-7, then no need to look in the last split at all... 
   *
   * @param scan_spec The base ScanSpec to start with
   * @return a new scan_spec object with a row interval matching this split
   */
  /*
  public ScanSpec createScanSpec(ScanSpec base_spec) {
	  
	  LOG.info("base_spec row regex: " + base_spec.getRow_regexp());
	  LOG.info("base_spec value regex: " + base_spec.getValue_regexp());
	  LOG.info("base_spec value intervals: " + base_spec.getRow_intervals());
	  
	    ScanSpec scan_spec = new ScanSpec(base_spec);
	    LOG.info("before unset scan_spec row regex: " + scan_spec.getRow_regexp());
	    LOG.info("before unset scan_spec value regex: " + scan_spec.getValue_regexp());
	    RowInterval interval = new RowInterval();
	    LOG.info("after unsetting scan_spec row regex: " + scan_spec.getRow_regexp());
	    LOG.info("before unset scan_spec value regex: " + scan_spec.getValue_regexp());

	    scan_spec.unsetRow_intervals();
	    
	    boolean returnEmptyScanspec = false;
	    try {
		    if (base_spec.isSetRow_intervals()) {
				for (RowInterval ri : base_spec.getRow_intervals()) {	// loop on all intervals
					
					String mStartRow;
						mStartRow 		 = (m_startrow==null) ? null : new String(m_startrow, "UTF-8");
						String mEndRow 	 = (m_endrow==null) ? null : new String(m_endrow, "UTF-8");

					
//					System.out.println("ri is : "+ri);
					if( (mStartRow!=null && ri.getEnd_row()!=null && ri.getEnd_row().compareTo(mStartRow)<0)  ||  (mEndRow!=null && !mEndRow.isEmpty() && ri.getStart_row()!=null && ri.getStart_row().compareTo(mEndRow)>0) )	// compare start/end rows
					{//																		empty endRow = biggest element 					
						returnEmptyScanspec = true;
						System.out.println("found mismatching split and rowInterval: ");
						System.out.println(  (mStartRow!=null && ri.getEnd_row()!=null && ri.getEnd_row().compareTo(mStartRow)<0) ?
								"rowInterval end-row ("+ri.getEnd_row()+") < split-start-row ("+mStartRow+")" 
								: "rowInterval start-row ("+ri.getStart_row()+") > split-end-row ("+mEndRow+")");
					}
				}
		    }
	    } catch (UnsupportedEncodingException e) {
	    	e.printStackTrace();
	    }
	    
//		 System.out.println("returnEmptyScanspec : "+returnEmptyScanspec);
	    if(returnEmptyScanspec)
	    {
	    	interval.setStart_row("xxx");
			interval.setStart_rowIsSet(true);
			interval.setStart_inclusive(true);
			interval.setStart_inclusiveIsSet(true);
			
			interval.setEnd_row("xxx");
			interval.setEnd_rowIsSet(true);
			interval.setEnd_inclusive(true);
			interval.setEnd_inclusiveIsSet(true);
	    }
	    else
	    {
		    	try 
		    	{
		    	if(m_startrow != null && m_startrow.length > 0) {
						interval.setStart_row(new String(m_startrow, "UTF-8"));
					interval.setStart_rowIsSet(true);
					interval.setStart_inclusive(false);
					interval.setStart_inclusiveIsSet(true);
				}
				
				if(m_endrow != null && m_endrow.length > 0) {
					interval.setEnd_row(new String(m_endrow,"UTF-8"));
					interval.setEnd_rowIsSet(true);
					interval.setEnd_inclusive(true);
					interval.setEnd_inclusiveIsSet(true);
				}
				
				if (base_spec.isSetRow_intervals()) {
					for (RowInterval ri : base_spec.getRow_intervals()) {
						if (ri.isSetStart_row()) {
							if (m_startrow == null ||
									ri.getStart_row().compareTo(new String(m_startrow, "UTF-8")) > 0) {
								interval.setStart_row(ri.getStart_row());
								interval.setStart_rowIsSet(true);
								interval.setStart_inclusive( ri.isStart_inclusive() );
								interval.setStart_inclusiveIsSet(true);
							}
						}
						if (ri.isSetEnd_row()) {
							if ( m_endrow == null || m_endrow.length == 0 ||
									ri.getEnd_row().compareTo(new String(m_endrow,"UTF-8")) < 0)  {
								interval.setEnd_row(ri.getEnd_row());
								interval.setEnd_rowIsSet(true);
								interval.setEnd_inclusive( ri.isEnd_inclusive() );
								interval.setEnd_inclusiveIsSet(true);
							}
						}
						// Only allowing a single row interval
						break;
					}
				}
		    } catch (UnsupportedEncodingException e) {
	    		e.printStackTrace();
	    	}
	    }
	    
	    if(interval.isSetStart_row() || interval.isSetEnd_row()) {
	    	scan_spec.addToRow_intervals(interval);
	    	scan_spec.setRow_intervalsIsSet(true);
	    }
	    
	    if(base_spec.isSetRow_regexp()){
	    	scan_spec.setRow_regexpIsSet(true);
	    	scan_spec.setRow_regexp(base_spec.getRow_regexp());
	    }
	    	
	    
	    return scan_spec;
	  }
	  */

  /**
   * Reads the values of each field.
   *
   * @param in  The input to read from.
   * @throws IOException When reading the input fails.
   */
  public void readFields(DataInput in) throws IOException {
    m_tablename = Serialization.readByteArray(in);
    m_startrow = Serialization.readByteArray(in);
    m_endrow = Serialization.readByteArray(in);
    m_hostname = Serialization.toString(Serialization.readByteArray(in));
    
    // added by Sagy Drucker
    m_scanspec = ScanSpec.serializedTextToScanSpec(
    		Serialization.toString(Serialization.readByteArray(in))
    		);
  }

  /**
   * Writes the field values to the output.
   *
   * @param out  The output to write to.
   * @throws IOException When writing the values to the output fails.
   */
  public void write(DataOutput out) throws IOException {
    Serialization.writeByteArray(out, m_tablename);
    Serialization.writeByteArray(out, m_startrow);
    Serialization.writeByteArray(out, m_endrow);
    Serialization.writeByteArray(out, Serialization.toBytes(m_hostname));
    
    // added by Sagy Drucker
    Serialization.writeByteArray(out, m_scanspec.toSerializedText().getBytes());
  }

  /**
   * Returns the details about this instance as a string.
   *
   * @return The values of this instance as a string.
   * @see java.lang.Object#toString()
   */
  public String toString() {
    String start_str = new String();
    String end_str = new String();

    if (m_startrow != null)
      start_str = Serialization.toStringBinary(m_startrow);

    if (m_endrow != null)
      end_str = Serialization.toStringBinary(m_endrow);

    return m_hostname + ":" + start_str + "," + end_str;
  }

  public int compareTo(TableSplit split) {
    return Serialization.compareTo(getStartRow(), split.getStartRow());
  }

  /* XXX add equals for columns */
  public boolean equal(TableSplit split) {
    return Serialization.equals(m_tablename, split.m_tablename) &&
      Serialization.equals(m_startrow, split.m_startrow) &&
      Serialization.equals(m_endrow, split.m_endrow) &&
      m_hostname.equals(split.m_hostname);
  }

public ScanSpec getM_scanspec() {
	return m_scanspec;
}

public void setM_scanspec(ScanSpec m_scanspec) {
	this.m_scanspec = m_scanspec;
}

}