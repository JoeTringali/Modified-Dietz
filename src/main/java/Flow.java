import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Flow implements Writable 
{
	private Text cashFlowDate = new Text();
	private DoubleWritable cashFlow = new DoubleWritable();
	
	public Flow()
	{
	}
	
	public Flow(Text cashFlowDate,
		DoubleWritable cashFlow)
	{
		this.cashFlowDate = cashFlowDate;
		this.cashFlow = cashFlow;
	}
	
	public Text getCashFlowDate() 
	{
		return cashFlowDate;
	}
	
	public DoubleWritable getCashFlow() 
	{
		return cashFlow;
	}

	public void readFields(DataInput in) throws IOException
	{
		cashFlowDate.readFields(in);
		cashFlow.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException
	{
		cashFlowDate.write(out);
		cashFlow.write(out);
	}
	
	public String toString()
	{
		return cashFlowDate.toString() + "\t" + 
				cashFlow.toString();
	}
}
