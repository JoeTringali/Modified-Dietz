import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;


public class FlowSum implements Writable 
{
	private DoubleWritable cashFlow = new DoubleWritable();

	public FlowSum()
	{
	}
	
	public FlowSum(DoubleWritable cashFlow)
	{
		this.cashFlow = cashFlow;
	}
	
	public DoubleWritable getCashFlow() 
	{
		return cashFlow;
	}

	public void readFields(DataInput in) throws IOException
	{
		cashFlow.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException
	{
		cashFlow.write(out);
	}
	
	public String toString()
	{
		return cashFlow.toString();
	}
}
