import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Balance implements Writable
{
	private Text beginningDate = new Text();
	private DoubleWritable beginningBalance = new DoubleWritable();
	private Text endingDate = new Text();
	private DoubleWritable endingBalance = new DoubleWritable();
	private LongWritable days = new LongWritable();
	
	public Balance()
	{
	}
	
	public Balance(Text beginningDate,
		DoubleWritable beginningBalance,
		Text endingDate,
		DoubleWritable endingBalance,
		LongWritable days)
	{
		this.beginningDate = beginningDate;
		this.beginningBalance = beginningBalance;
		this.endingDate = endingDate;
		this.endingBalance = endingBalance;
		this.days = days;
	}
	
	public Text getBeginningDate() 
	{
		return beginningDate;
	}
	
	public DoubleWritable getBeginningBalance() 
	{
		return beginningBalance;
	}

	public Text getEndingDate() 
	{
		return endingDate;
	}

	public DoubleWritable getEndingBalance() 
	{
		return endingBalance;
	}

	public LongWritable getDays() 
	{
		return days;
	}

	public void readFields(DataInput in) throws IOException
	{
		beginningDate.readFields(in);
		beginningBalance.readFields(in);
		endingDate.readFields(in);
		endingBalance.readFields(in);
		days.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException
	{
		beginningDate.write(out);
		beginningBalance.write(out);
		endingDate.write(out);
		endingBalance.write(out);
		days.write(out);
	}
	
	public String toString()
	{
		return beginningDate.toString() + "\t" + 
			beginningBalance.toString() + "\t" +
			endingDate.toString() + "\t" +
			endingBalance.toString() + "\t" +
			days.toString();
	}
}
