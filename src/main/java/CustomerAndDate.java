import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomerAndDate implements WritableComparable<CustomerAndDate> 
{
	private Text customerId = new Text();
	private Text date = new Text();

	public CustomerAndDate()
	{
	}
	
	public CustomerAndDate(
			Text customerId, Text date)
	{
		this.customerId = customerId;
		this.date = date;
	}
	
	public Text getCustomerId()
	{
		return customerId;
	}
	
	public Text getDate()
	{
		return date;
	}
	
	public void readFields(DataInput in) throws IOException
	{
		customerId.readFields(in);
		date.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException
	{
		customerId.write(out);
		date.write(out);
	}
	
	public int compareTo(CustomerAndDate o) 
	{
        int cmp = customerId.toString().compareTo(o.customerId.toString());
        if (cmp == 0)
        {
        	cmp = date.toString().compareTo(o.date.toString());
        }
        return cmp;
     }
	
	public String toString()
	{
		return customerId.toString() + "\t" + 
			date.toString();
	}
	
	public boolean equals(CustomerAndDate other)
	{
		return (customerId.toString().equals(other.customerId.toString()) && 
			    date.toString().equals(other.date.toString()));
	}
	
	public int hashCode()
	{
		return customerId.toString().hashCode();
	}
}
