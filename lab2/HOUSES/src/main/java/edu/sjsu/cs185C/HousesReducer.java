package edu.sjsu.cs185C;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
import java.io.IOException;

public class HousesReducer  extends Reducer <Text,Text,Text,FloatWritable> {

    private static Log log = LogFactory.getLog(HousesReducer.class);

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //TODO: Loop through the input values, for each of them,
        //      extract the fields from the input value,
        //      use the fields salePrice and lotArea to calculate the salePrice-per-square,
        //      keep track of the summation of the salePrice
        //      if salePrice or salePrice the min or max value so far, remember the record's
        //      other fields.
        //      When the loop finishes, calculate the mean of the salePrice, salePrice-per-square,
        //      for this neightbor
        //      write to context with key as Per-house-max/min/mean followed by its neightbor.
        //      If it is for max and min salePrice, append the key with other fields info that we remember from the loop too.
        //      The value will be writen to context is the max/min/mean salePrice
        //      Similarly write the (key, value) for Per-Square-foot salePrice as well.
        //      Please check the output in the lab instructions for exact format.
    }
}
