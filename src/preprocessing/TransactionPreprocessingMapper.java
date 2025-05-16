package preprocessing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

public class TransactionPreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {
    private SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
    private Set<String> uniqueRefs = new HashSet<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("Transaction_Date")) {
            return;
        }


        String[] fields = value.toString().split(",");
        if (fields.length != 6) {
            return; 
        }

        String transactionDate = fields[0];
        String transactionRef = fields[1];
        String withdrawal = fields[3];
        String deposit = fields[4];


        // Preprocessing steps
        try {
            dateFormatter.parse(transactionDate);
        } catch (ParseException e) {
            return;
        }

        if (uniqueRefs.contains(transactionRef)) {
            return;
        } else {
            uniqueRefs.add(transactionRef);
        }

        double withdrawalAmount = Double.parseDouble(withdrawal);
        double depositAmount = Double.parseDouble(deposit);
        if (withdrawalAmount == 0.0 && depositAmount == 0.0) {
            return; 
        }

        context.write(new Text(transactionRef), value);
    }
}
