package edu.sjsu.cs185C;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Arrays;

public class HousesMapper  extends Mapper <LongWritable,Text,Text,Text> {

	private static Log log = LogFactory.getLog(HousesMapper.class);

	/*
    0  1          2        3           4       5      6     7        8           9         10        11        12           13         14         15       16         17          18          19        20           21        22       23          24          25         26         27        28        29         30       31       32           33           34         35           36         37        38          39      40        41         42         43       44       45           46        47           48           49       50       51           52           53          54           55         56         57          58         59          60           61         62         63         64         65         66         67          68            69        70          71       72     73    74          75      76     77     78       79            80 
    Id,MSSubClass,MSZoning,LotFrontage,LotArea,Street,Alley,LotShape,LandContour,Utilities,LotConfig,LandSlope,Neighborhood,Condition1,Condition2,BldgType,HouseStyle,OverallQual,OverallCond,YearBuilt,YearRemodAdd,RoofStyle,RoofMatl,Exterior1st,Exterior2nd,MasVnrType,MasVnrArea,ExterQual,ExterCond,Foundation,BsmtQual,BsmtCond,BsmtExposure,BsmtFinType1,BsmtFinSF1,BsmtFinType2,BsmtFinSF2,BsmtUnfSF,TotalBsmtSF,Heating,HeatingQC,CentralAir,Electrical,1stFlrSF,2ndFlrSF,LowQualFinSF,GrLivArea,BsmtFullBath,BsmtHalfBath,FullBath,HalfBath,BedroomAbvGr,KitchenAbvGr,KitchenQual,TotRmsAbvGrd,Functional,Fireplaces,FireplaceQu,GarageType,GarageYrBlt,GarageFinish,GarageCars,GarageArea,GarageQual,GarageCond,PavedDrive,WoodDeckSF,OpenPorchSF,EnclosedPorch,3SsnPorch,ScreenPorch,PoolArea,PoolQC,Fence,MiscFeature,MiscVal,MoSold,YrSold,SaleType,SaleCondition,SalePrice
	 */
	private static final int lotAreaIdx = 4;
	private static final int neighborhoodIdx = 12;
	private static final int yearBuiltIdx = 19;
	private static final int yearRemodAddIdx = 20;
	private static final int yrSoldIdx = 77;
	private static final int salePriceIdx = 80;


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//skip very first record (schema line)
		if (key.get() == 0) {
			return;
		}

		if(key.get() > 0)
		{
			String[] values = value.toString().split(",");
			if(values.length != 81)
			{
				log.info("Missing elements");
				return;
			}


			Text neighborhood = new Text(values[neighborhoodIdx]);

			if(neighborhood.toString().equals(""))
			{
				log.info("Neightborhood value missing");
				return;
			}



			try
			{
				Integer hold = Integer.parseInt(values[lotAreaIdx]);
				hold = Integer.parseInt(values[yearBuiltIdx]);
				hold = Integer.parseInt(values[yearRemodAddIdx]);
				hold = Integer.parseInt(values[yrSoldIdx]);
				hold = Integer.parseInt(values[salePriceIdx]);
			}
			catch(NumberFormatException e)
			{
				log.info("Missing or incorrect values for lotArea, yearBuilt, yearRemodAdd, yrSold, and/or salePrice");
				return;
			}

			
			if(Integer.parseInt(values[lotAreaIdx])<=0 ||Integer.parseInt(values[yearBuiltIdx]) <=0
					|| Integer.parseInt(values[yearRemodAddIdx]) <=0 || Integer.parseInt(values[yrSoldIdx])<=0
					|| Integer.parseInt(values[salePriceIdx]) <=0)
			{
				log.info("Can't have negative values for lotArea, yearBuilt, yearRemodAdd, yrSold, and/or salePrice");
				return;
			}
			else
			{
				Text rest = new Text(values[lotAreaIdx] + "," + values[yearBuiltIdx] + "," + values[yearRemodAddIdx]+
						"," + values[yrSoldIdx] + "," +values[salePriceIdx]);
				context.write(neighborhood, rest);
			}
			


			//System.out.println("---------" + neighborhood.toString() + "------" +rest.toString() + "---------");

		}

		// TODO: read out the needed fields from the input value, and validate the fields contain valid info
		//       emit key-value as (neighborhood, string-with-all-other-fields-info) 

	}
}
