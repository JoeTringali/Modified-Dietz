import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CalculateModifiedDietz 
{
	public static class GetBalanceByCustomerAndDateMapper
		extends Mapper<Object, Text, CustomerAndDate, DoubleWritable>
	{
		private Text customerId = new Text();
		private Text balanceDate = new Text();
		private DoubleWritable balance = new DoubleWritable();
		
		
		@Override
		public void map(Object key, Text value,
			Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			StringTokenizer s = new StringTokenizer(line, ",");
			customerId.set(s.nextToken());
			s.nextToken();
			s.nextToken();
			balanceDate.set(s.nextToken());
			s.nextToken();
			balance.set(Double.parseDouble(s.nextToken()));
			context.write(new CustomerAndDate(
					customerId, 
					balanceDate),
					balance);
		}
	}
	
	public static class GetBalanceByCustomerAndDateReducer extends Reducer<
			CustomerAndDate, DoubleWritable, 
			CustomerAndDate, DoubleWritable>
	{
		private DoubleWritable balance = new DoubleWritable();

		@Override
		public void reduce(CustomerAndDate key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException
		{
			double balance = 0;
			for (DoubleWritable value : values)
			{
				balance += value.get();
			}
			this.balance.set(balance);
			context.write(key, this.balance);
		}	
	}
	
	public static class GetBalanceByCustomerMapper
		extends Mapper<Object, Text, Text, Balance>
	{
		private Text customerId = new Text();
		private Text balanceDate = new Text();
		private DoubleWritable balance = new DoubleWritable();
		private LongWritable days = new LongWritable(0);
		
		@Override
		public void map(Object key, Text value,
			Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			StringTokenizer s = new StringTokenizer(line, "\t");
			customerId.set(s.nextToken());
			balanceDate.set(s.nextToken());
			balance.set(Double.parseDouble(s.nextToken()));
			Balance record = new Balance(balanceDate, balance, balanceDate, balance, days);
			context.write(customerId, record);
		}
	}

	public static class GetBalanceByCustomerReducer extends Reducer<
		Text, Balance, 
		Text, Balance>
	{
		private Text beginningDate = new Text();
		private DoubleWritable beginningBalance = new DoubleWritable();
		private Text endingDate = new Text();
		private DoubleWritable endingBalance = new DoubleWritable();
		private LongWritable days = new LongWritable();
		
		@Override
		public void reduce(Text key, Iterable<Balance> values,
				Context context) throws IOException, InterruptedException
		{
			String beginningDate = null;
			double beginningBalance = 0;
			String endingDate = null;
			double endingBalance = 0;
			String date;
			int cmp;
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
			LocalDate beginningLocalDate;
			LocalDate endingLocalDate;
			for (Balance value : values)
			{
				cmp = 0;
				date = value.getBeginningDate().toString();
				if (beginningDate != null)
				{
					cmp = date.compareTo(beginningDate);
				}
				if (beginningDate == null ||
					cmp < 0)
				{
					beginningDate = date;
					beginningBalance = value.getBeginningBalance().get();
				}
				else if (cmp == 0)
				{
					beginningBalance += value.getBeginningBalance().get();
				}
				cmp = 0;
				date = value.getEndingDate().toString();
				if (endingDate != null)
				{
					cmp = date.compareTo(endingDate);
				}
				if (endingDate == null ||
					cmp > 0)
				{
					endingDate = date;
					endingBalance = value.getEndingBalance().get();
				}
				else if (cmp == 0)
				{
					endingBalance += value.getEndingBalance().get();
				}
			}
			this.beginningDate.set(beginningDate);
			this.beginningBalance.set(beginningBalance);
			this.endingDate.set(endingDate);
			this.endingBalance.set(endingBalance);
			beginningLocalDate = LocalDate.parse(beginningDate, dtf);
			endingLocalDate = LocalDate.parse(endingDate, dtf);
			this.days.set(ChronoUnit.DAYS.between(beginningLocalDate, endingLocalDate));	
			context.write(key, 
				new Balance(this.beginningDate,
						this.beginningBalance,
						this.endingDate,
						this.endingBalance,
						this.days));

		}	
	}
	
	public static class GetCashFlowByCustomerMapper
	extends Mapper<Object, Text, Text, DoubleWritable>
	{
		private Text customerId = new Text();
		private DoubleWritable cashFlow = new DoubleWritable();
		
		@Override
		public void map(Object key, Text value,
			Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			StringTokenizer s = new StringTokenizer(line, ",");
			customerId.set(s.nextToken());
			s.nextToken();
			s.nextToken();
			s.nextToken();
			s.nextToken();
			cashFlow.set(Double.parseDouble(s.nextToken()));
			context.write(customerId, cashFlow);
		}
	}
	
	public static class GetCashFlowByCustomerReducer extends Reducer<
	Text, DoubleWritable, 
	Text, DoubleWritable>
	{
		private DoubleWritable cashFlow = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException
		{
			double cashFlow = 0;
			for (DoubleWritable value : values)
			{
				cashFlow += value.get();
			}
			this.cashFlow.set(cashFlow);
			context.write(key, this.cashFlow);
		}
	}

	public static class GetCashFlowByCustomerAndDateMapper
	extends Mapper<Object, Text, CustomerAndDate, DoubleWritable>
	{
		private Text customerId = new Text();
		private Text cashFlowDate = new Text();
		private DoubleWritable cashFlow = new DoubleWritable();
		
		@Override
		public void map(Object key, Text value,
			Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			StringTokenizer s = new StringTokenizer(line, ",");
			customerId.set(s.nextToken());
			s.nextToken();
			cashFlowDate.set(s.nextToken());
			s.nextToken();
			s.nextToken();
			cashFlow.set(Double.parseDouble(s.nextToken()));
			context.write(new CustomerAndDate(
					customerId, 
					cashFlowDate),
					cashFlow);
		}
	}
	
	public static class GetCashFlowByCustomerAndDateReducer extends Reducer<
			CustomerAndDate, DoubleWritable, 
			CustomerAndDate, DoubleWritable>
	{
		private DoubleWritable cashFlow = new DoubleWritable();
		

		@Override
		public void reduce(CustomerAndDate key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException
		{
			double cashFlow = 0;
			for (DoubleWritable value : values)
			{
				cashFlow += value.get();
			}
			this.cashFlow.set(cashFlow);
			context.write(key, this.cashFlow);
		}
	}
	
	public static class JoinGroupingComparator extends WritableComparator
	{
		public JoinGroupingComparator()
		{
			super(BalanceFlowKey.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b)
		{
			BalanceFlowKey first = (BalanceFlowKey) a;
			BalanceFlowKey second = (BalanceFlowKey) b;
			return first.getCustomerId().toString().compareTo(second.getCustomerId().toString());
		}
	}
	
	public static class JoinSortingComparator extends WritableComparator
	{
		public JoinSortingComparator()
		{
			super(BalanceFlowKey.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b)
		{
			BalanceFlowKey first = (BalanceFlowKey) a;
			BalanceFlowKey second = (BalanceFlowKey) b;
			return first.compareTo(second);
		}
	}
	
	public static class JobRunner implements Runnable
	{
		private JobControl _jobControl;
		
		public JobRunner(JobControl jobControl)
		{
			_jobControl = jobControl;
		}
		
		public void run()
		{
			_jobControl.run();
		}
	}
	
	public static class BalanceToJoinMapper
		extends Mapper<Object, Text, BalanceFlowKey, JoinGenericWritable>
	{
		private Text customerId = new Text();
		private Text beginningDate = new Text();
		private DoubleWritable beginningBalance = new DoubleWritable();
		private Text endingDate = new Text();
		private DoubleWritable endingBalance = new DoubleWritable();
		private LongWritable days = new LongWritable();
		
		@Override
		public void map(Object key, Text value,
			Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			StringTokenizer s = new StringTokenizer(line, "\t");
			customerId.set(s.nextToken());
			beginningDate.set(s.nextToken());
			beginningBalance.set(Double.parseDouble(s.nextToken()));
			endingDate.set(s.nextToken());
			endingBalance.set(Double.parseDouble(s.nextToken()));
			days.set(Long.parseLong(s.nextToken()));
			
			BalanceFlowKey recordKey = 
				new BalanceFlowKey(customerId, BalanceFlowKey.BALANCE);
			Balance record = new Balance(beginningDate, beginningBalance, endingDate, endingBalance, days);
			JoinGenericWritable genericRecord = new JoinGenericWritable(record);
			context.write(recordKey, genericRecord);
		}
	}
	
	public static class FlowSumToJoinMapper
	extends Mapper<Object, Text, BalanceFlowKey, JoinGenericWritable>
	{
		private Text customerId = new Text();
		private DoubleWritable cashFlow = new DoubleWritable();
		
		@Override
		public void map(Object key, Text value,
			Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			StringTokenizer s = new StringTokenizer(line, "\t");
			customerId.set(s.nextToken());
			cashFlow.set(Double.parseDouble(s.nextToken()));
			
			BalanceFlowKey recordKey = 
				new BalanceFlowKey(customerId, BalanceFlowKey.FLOW_SUM);
			FlowSum record = new FlowSum(cashFlow);
			JoinGenericWritable genericRecord = new JoinGenericWritable(record);
			context.write(recordKey, genericRecord);
		}
	}
	
	public static class FlowToJoinMapper
	extends Mapper<Object, Text, BalanceFlowKey, JoinGenericWritable>
	{
		private Text customerId = new Text();
		private Text cashFlowDate = new Text();
		private DoubleWritable cashFlow = new DoubleWritable();
		
		@Override
		public void map(Object key, Text value,
			Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			StringTokenizer s = new StringTokenizer(line, "\t");
			customerId.set(s.nextToken());
			cashFlowDate.set(s.nextToken());
			cashFlow.set(Double.parseDouble(s.nextToken()));
			
			BalanceFlowKey recordKey = 
				new BalanceFlowKey(customerId, BalanceFlowKey.FLOW);
			Flow record = new Flow(cashFlowDate, cashFlow);
			JoinGenericWritable genericRecord = new JoinGenericWritable(record);
			context.write(recordKey, genericRecord);
		}
	}
	
	public static class JoinReducer extends
		Reducer<BalanceFlowKey, JoinGenericWritable, NullWritable, Text>
	{
		StringBuilder output;
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		
		@Override
		public void reduce(BalanceFlowKey key, Iterable<JoinGenericWritable> values, Context context)
			throws IOException, InterruptedException
		{
			Writable record;
			Balance balanceRecord;
			FlowSum flowSumRecord;
			Flow flowRecord;
			String customerId = null;
			String beginningDate = null;
			double beginningBalance = 0;
			String endingDate = null;
			double endingBalance = 0;
			long days = 0;
			double flowSum = 0;
			double gainOrLoss = 0;
			String cashFlowDate;
			double cashFlow = 0;
			LocalDate beginningLocalDate;
			LocalDate endingLocalDate;
			LocalDate cashFlowLocalDate;
			long daysFromStartOfPeriod = 0;
			double weightFactor;
			double weightedCashFlow;
			double averageCapital = 0;
			double portfolioReturn = 0;
			output = new StringBuilder();
			for (JoinGenericWritable value : values)
			{
				record = value.get();
				if (customerId == null &&
					key.getCustomerId().toString() != null)
				{
					customerId = key.getCustomerId().toString();
				}
				if (key.getRecordType().equals(BalanceFlowKey.BALANCE))
				{
					balanceRecord = (Balance) record;
					if (beginningDate == null &&
						balanceRecord.getBeginningDate() != null)
					{
						beginningDate = balanceRecord.getBeginningDate().toString();
						beginningBalance = balanceRecord.getBeginningBalance().get();
						endingDate = balanceRecord.getEndingDate().toString();
						endingBalance = balanceRecord.getEndingBalance().get();
						days = balanceRecord.getDays().get();
					}
					
				}
				else if (key.getRecordType().equals(BalanceFlowKey.FLOW_SUM))
				{
					flowSumRecord = (FlowSum) record;
					flowSum = flowSumRecord.getCashFlow().get();
					gainOrLoss = endingBalance - beginningBalance - flowSum;
				}
				else if (key.getRecordType().equals(BalanceFlowKey.FLOW))
				{
					flowRecord = (Flow) record;
					cashFlowDate = flowRecord.getCashFlowDate().toString();
					cashFlow = flowRecord.getCashFlow().get();
					if (beginningDate != null)
					{
						beginningLocalDate = LocalDate.parse(beginningDate, dtf);
						endingLocalDate = LocalDate.parse(endingDate, dtf);
						cashFlowLocalDate = LocalDate.parse(cashFlowDate, dtf);
						daysFromStartOfPeriod = ChronoUnit.DAYS.between(beginningLocalDate, cashFlowLocalDate);
						weightFactor = (days - daysFromStartOfPeriod) * 1.0 / days;
						weightedCashFlow = weightFactor * cashFlow;
						averageCapital += weightedCashFlow;
						portfolioReturn = gainOrLoss / averageCapital;
					}
				}
			}
			if (customerId != null)
			{
				output.append(customerId + ",");
				output.append(beginningDate + ",");
				output.append(beginningBalance + ",");
				output.append(endingDate + ",");
				output.append(endingBalance + ",");
				output.append(flowSum + ",");
				output.append(gainOrLoss + ",");
				output.append(averageCapital + ",");
				output.append(portfolioReturn);
				context.write(NullWritable.get(), new Text(output.toString()));
			}
		}
	}

	public static void main(String args[]) throws Exception
	{
		Configuration config = new Configuration();
		Path output = new Path(args[2]);

		Job job1 = Job.getInstance(config, "Balance by Customer and Date");
		job1.setJarByClass(CalculateModifiedDietz.class);
		job1.setMapperClass(GetBalanceByCustomerAndDateMapper.class);
		job1.setCombinerClass(GetBalanceByCustomerAndDateReducer.class);
		job1.setReducerClass(GetBalanceByCustomerAndDateReducer.class);
		job1.setOutputKeyClass(CustomerAndDate.class);
		job1.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job1, new Path(args[0])); 
	    FileOutputFormat.setOutputPath(job1, new Path(output, "balanceByCustomerAndDate"));
		
	    Job job2 = Job.getInstance(config, "Balance by Customer");
		job2.setJarByClass(CalculateModifiedDietz.class);
		job2.setMapperClass(GetBalanceByCustomerMapper.class);
		job2.setCombinerClass(GetBalanceByCustomerReducer.class);
		job2.setReducerClass(GetBalanceByCustomerReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Balance.class);
	    FileInputFormat.addInputPath(job2, new Path(output, "balanceByCustomerAndDate")); 
	    FileOutputFormat.setOutputPath(job2, new Path(output, "balanceByCustomer"));	    

		Job job3 = Job.getInstance(config, "Cash Flow by Customer");
		job3.setJarByClass(CalculateModifiedDietz.class);
		job3.setMapperClass(GetCashFlowByCustomerMapper.class);
		job3.setCombinerClass(GetCashFlowByCustomerReducer.class);
		job3.setReducerClass(GetCashFlowByCustomerReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job3, new Path(args[1])); 
	    FileOutputFormat.setOutputPath(job3, new Path(output, "cashFlowByCustomer"));
		
		Job job4 = Job.getInstance(config, "Cash Flow by Customer and Date");
		job4.setJarByClass(CalculateModifiedDietz.class);
		job4.setMapperClass(GetCashFlowByCustomerAndDateMapper.class);
		job4.setCombinerClass(GetCashFlowByCustomerAndDateReducer.class);
		job4.setReducerClass(GetCashFlowByCustomerAndDateReducer.class);
		job4.setOutputKeyClass(CustomerAndDate.class);
		job4.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job4, new Path(args[1])); 
	    FileOutputFormat.setOutputPath(job4, new Path(output, "cashFlowByCustomerAndDate"));

		Job job5 = Job.getInstance(config, "Calculate Modified Dietz");
		job5.setJarByClass(CalculateModifiedDietz.class);
		job5.setInputFormatClass(TextInputFormat.class);
		job5.setOutputFormatClass(TextOutputFormat.class);
		job5.setMapOutputKeyClass(BalanceFlowKey.class);
		job5.setMapOutputValueClass(JoinGenericWritable.class);
		job5.setReducerClass(JoinReducer.class);
		job5.setSortComparatorClass(JoinSortingComparator.class);
		job5.setGroupingComparatorClass(JoinGroupingComparator.class);
		job5.setOutputKeyClass(NullWritable.class);
		job5.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job5, new Path(output, "balanceByCustomer"), TextInputFormat.class, BalanceToJoinMapper.class);
		MultipleInputs.addInputPath(job5, new Path(output, "cashFlowByCustomer"), TextInputFormat.class, FlowSumToJoinMapper.class);
		MultipleInputs.addInputPath(job5, new Path(output, "cashFlowByCustomerAndDate"), TextInputFormat.class, FlowToJoinMapper.class);
	    FileOutputFormat.setOutputPath(job5, new Path(output, "modifiedDietz"));
	    
	    ControlledJob controlledJob1 = new ControlledJob(job1, null);
	    ControlledJob controlledJob2 = new ControlledJob(job2, null);
	    controlledJob2.addDependingJob(controlledJob1);
	    ControlledJob controlledJob3 = new ControlledJob(job3, null);
	    ControlledJob controlledJob4 = new ControlledJob(job4, null);
	    ControlledJob controlledJob5 = new ControlledJob(job5, null);
	    controlledJob5.addDependingJob(controlledJob2);
	    controlledJob5.addDependingJob(controlledJob3);
	    controlledJob5.addDependingJob(controlledJob4);
	    
	    JobControl jobControl = new JobControl("Calculate Modified Dietz");
	    jobControl.addJob(controlledJob1);
	    jobControl.addJob(controlledJob2);
	    jobControl.addJob(controlledJob3);
	    jobControl.addJob(controlledJob4);
	    jobControl.addJob(controlledJob5);
	    
	    JobRunner jobRunner = new JobRunner(jobControl);
	    Thread thread = new Thread(jobRunner);
	    thread.start();
	    
	    while(!jobControl.allFinished())
	    {
	    	System.out.println("System still running...");
	    	Thread.sleep(5000);
	    }

	    System.exit(0);
	}
}
