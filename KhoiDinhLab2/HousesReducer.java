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


		String[] val;
		int size= 0;
		float average = 0;
		float sum = 0;
		float max = Float.MIN_VALUE;
		float min = Float.MAX_VALUE;

		float square_min = Float.MAX_VALUE;
		float square_max = Float.MIN_VALUE;
		float square_avg = 0;
		float square_sum=0;

		int lotArea_min = 0;
		int yearBuilt_min = 0;
		int yearRemod_min =0;
		int yearSold_min =0;

		int lotArea_max = 0;
		int yearBuilt_max = 0;
		int yearRemod_max =0;
		int yearSold_max =0;
		
		
		int lotArea_min2 = 0;
		int yearBuilt_min2 = 0;
		int yearRemod_min2 =0;
		int yearSold_min2 =0;

		int lotArea_max2 = 0;
		int yearBuilt_max2 = 0;
		int yearRemod_max2 =0;
		int yearSold_max2 =0;


		for(Text each: values)
		{
			val = each.toString().split(",");
			int lotArea = Integer.parseInt(val[0]);
			int yearBuilt = Integer.parseInt(val[1]);
			int yearRemod = Integer.parseInt(val[2]);
			int yearSold = Integer.parseInt(val[3]);
			int salePrice = Integer.parseInt(val[4]);


			if(salePrice > max)
			{
				max = salePrice;
				lotArea_max = lotArea;
				yearBuilt_max = yearBuilt;
				yearRemod_max =yearRemod;
				yearSold_max =yearSold;
			}
			if(salePrice < min)
			{
				min = salePrice;
				lotArea_min = lotArea;
				yearBuilt_min = yearBuilt;
				yearRemod_min =yearRemod;
				yearSold_min =yearSold;
			}

			sum =  sum + salePrice;
			size++;
			
			
			Float squareVal = Float.parseFloat(salePrice + "")/ Float.parseFloat(lotArea + "");
			if( squareVal> square_max)
			{
				square_max= squareVal;
				lotArea_max2 = lotArea;
				yearBuilt_max2 = yearBuilt;
				yearRemod_max2 =yearRemod;
				yearSold_max2 =yearSold;
			}
			if(squareVal < square_min)
			{
				square_min = squareVal;
				lotArea_min2 = lotArea;
				yearBuilt_min2 = yearBuilt;
				yearRemod_min2 =yearRemod;
				yearSold_min2 =yearSold;
			}

			square_sum = square_sum + squareVal;
		}
			
		square_avg = square_sum/size;
		average = sum/ size;
		
		
		context.write(new Text("Per-house-max    " + key + "   " + lotArea_max + "   " +
				yearBuilt_max + "   " + yearRemod_max + "   " + yearSold_max + "   "), new FloatWritable(max));

		context.write(new Text("Per-house-mean   " +key), new FloatWritable(average));

		context.write(new Text("Per-house-min    " + key + "   " + lotArea_min + "   " + yearBuilt_min 
				+ "   " + yearRemod_min + "   " + yearSold_min + "   "), new FloatWritable(min));


		context.write(new Text("Per-square-foot-max    " + key + "   " + lotArea_max2 + "   " +
				yearBuilt_max2 + "   " + yearRemod_max2 + "   " + yearSold_max2 + "   "), new FloatWritable(square_max));

		context.write(new Text("Per-square-foot-mean   " +key), new FloatWritable(square_avg));

		context.write(new Text("Per-square-foot-min    " + key + "   " + lotArea_min2 + "   " + yearBuilt_min2 
				+ "   " + yearRemod_min2 + "   " + yearSold_min2 + "   "), new FloatWritable(square_min));
	}
}
