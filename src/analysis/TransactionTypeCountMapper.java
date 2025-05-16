package analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TransactionTypeCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text transactionType = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
    
        if (line.contains("Transaction_Date")) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length < 5) {
            return; 
        }

        double withdrawal = Double.parseDouble(fields[3]);
        double deposit = Double.parseDouble(fields[4]);

        if (withdrawal > 0.0) {
            transactionType.set("Withdrawal");
            context.write(transactionType, one);
        } 
        if (deposit > 0.0) {
            transactionType.set("Deposit");
            context.write(transactionType, one);
        }
    }
}
