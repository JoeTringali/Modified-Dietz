import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class BalanceFlowKey implements WritableComparable<BalanceFlowKey>
{
	public static final IntWritable BALANCE = new IntWritable(0);
	public static final IntWritable FLOW_SUM = new IntWritable(1);
	public static final IntWritable FLOW = new IntWritable(2);
	
	private Text customerId = new Text();
	private IntWritable recordType = new IntWritable();
	
	public BalanceFlowKey()
	{
	}
	
	public BalanceFlowKey(Text customerId, IntWritable recordType)
	{
		this.customerId = customerId;
		this.recordType = recordType;
	}
	
	public Text getCustomerId()
	{
		return customerId;
	}
	
	public IntWritable getRecordType()
	{
		return recordType;
	}
	
	public void readFields(DataInput in) throws IOException
	{
		customerId.readFields(in);
		recordType.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException
	{
		customerId.write(out);
		recordType.write(out);
	}
	
	public int compareTo(BalanceFlowKey o) 
	{
        int cmp = customerId.toString().compareTo(o.customerId.toString());
        if (cmp == 0)
        {
        	cmp = recordType.toString().compareTo(o.recordType.toString());
        }
        return cmp;
     }
	
	public String toString()
	{
		return customerId.toString() + "\t" + 
			recordType.toString();
	}
	
	public boolean equals(BalanceFlowKey other)
	{
		return (customerId.toString().equals(other.customerId.toString()) && 
			    recordType.toString().equals(other.recordType.toString()));
	}
	
	public int hashCode()
	{
		return customerId.toString().hashCode();
	}
}
