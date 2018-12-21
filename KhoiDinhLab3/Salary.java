package edu.sjsu.cs185C;

import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import static org.apache.spark.sql.functions.col;

public class Salary {

    // We will use the following three data sets, make sure your input arguements follows this order.
    // san-jose-2016.csv san-jose-2017.csv san-francisco-2017.small.csv
    public static void main(String[] args) throws Exception {
        //Input files
        String fileSJ16= args[0];
        String fileSJ17= args[1];
        String fileSF17= args[2];
        // Create a Java Spark Context.
        SparkSession spark = SparkSession
                .builder()
                .appName("CaliforniaSalary")
                .getOrCreate();

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("employeeName", DataTypes.StringType, true),
                DataTypes.createStructField("jobTitle", DataTypes.StringType, true),
                DataTypes.createStructField("basePay", DataTypes.FloatType, true),
                DataTypes.createStructField("overtimePay", DataTypes.FloatType, true),
                DataTypes.createStructField("otherPay", DataTypes.FloatType, true),
                DataTypes.createStructField("benefits", DataTypes.FloatType, true),
                DataTypes.createStructField("totalPay", DataTypes.FloatType, true),
                DataTypes.createStructField("totalPayAndBenefits", DataTypes.FloatType, true),
                DataTypes.createStructField("year", DataTypes.IntegerType, true),
                DataTypes.createStructField("notes", DataTypes.StringType, true),
                DataTypes.createStructField("agency", DataTypes.StringType, true),
                DataTypes.createStructField("status", DataTypes.StringType, true));
        StructType salarySchema = DataTypes.createStructType(fields);

        //Load input data and create 3 Dataset<Row>: salary for SJ 2016, SJ 2017 and SF 2017
        Dataset<Row> salaryDFSJ16 = spark.read().option("header", true).schema(salarySchema).csv(fileSJ16);
        Dataset<Row> salaryDFSJ17 = spark.read().option("header", true).schema(salarySchema).csv(fileSJ17);
        Dataset<Row> salaryDFSF17 = spark.read().option("header", true).schema(salarySchema).csv(fileSF17);

        //Using the following salaryEncoder, to create typed Dataset<SalaryRecord> for above inputs
        //Encoder<SalaryRecord> salaryEncoder = Encoders.bean(SalaryRecord.class);
        Encoder<SalaryRecord> salaryEncoder = Encoders.bean(SalaryRecord.class);
        Dataset<SalaryRecord> salaryDSSJ16 = salaryDFSJ16.as(salaryEncoder);
        Dataset<SalaryRecord> salaryDSSJ17 = salaryDFSJ17.as(salaryEncoder);
        Dataset<SalaryRecord> salaryDSSF17 = salaryDFSF17.as(salaryEncoder);

        //Hint: If you like to use spark.sql, you can choose create 3 table views using
        // createOrReplaceTempView, if not, Dataset APIs are able to achieve the same goal.

        long totalRecordCount = 0;
        long totalJobTitleCount = 0;
        //TODO 1: count the distinct jobTitles in 2017 San Jose data set
        
        totalRecordCount = salaryDSSJ17.count();
        totalJobTitleCount = salaryDSSJ17.select("jobTitle").distinct().count();
        System.out.println("---2017 San Jose: Total Record Count : " + totalRecordCount + ", Total JobTile Count: " + totalJobTitleCount);
        
        //TODO 2: find and print the top 3 records with lowest totalPayAndBenefits in 2017 San Jose data set
        System.out.println("---2017 San Jose: 3 records with lowest totalPayAndBenefits");
        salaryDSSJ17.orderBy(col("totalPayAndBenefits").asc()).show(3);
        
        //TODO 3: find and print the top 3 records with highest totalPayAndBenefits in 2017 San Jose data set
        System.out.println("---2017 San Jose: 3 record with highest totalPayAndBenefits");
        salaryDSSJ17.orderBy(col("totalPayAndBenefits").desc()).show(3);
        
        //TODO 4: find and print the records with highest overtimePay in 2017 San Jose data set
        System.out.println("---2017 San Jose: record with highest overtime pay");
        Dataset<SalaryRecord> highestOvertime = salaryDSSJ17.orderBy(col("overtimePay").desc());
        highestOvertime.show(1);
        
        //TODO 5: find and print  the highest benefit records in 2017 San Jose data set
        System.out.println("---2017 San Jose: record with highest benefit pay");
        Dataset<SalaryRecord> highestBenefits = salaryDSSJ17.orderBy(col("benefits").desc());
        highestBenefits.show(1);
        
        //TODO 6: find and print the lowest totalPayAndBenefit records in 2017 San Francisco data set
        System.out.println("---2017 San Francisco: 3 records with lowest totalPayAndBenefits");
        salaryDSSF17.orderBy(col("totalPayAndBenefits").asc(), col("employeeName").asc()).show(3);
        
        //TODO 7: find and print the highest totalPayAndBenefit records in 2017 San Francisco data set
        System.out.println("---2017 San Francisco 3 records with highest totalPayAndBenefits");
        Dataset<SalaryRecord> highestTotal = salaryDSSF17.orderBy(col("totalPayAndBenefits").desc(), col("employeeName").desc());
        highestTotal.show(3);
        
        //TODO 8: In San Jose 2017 data set, find the highest totalPayAndBenefits for the jobTitle "Police Officer"
        String jobTitlePO = "Police Officer";
        System.out.println("---2017 San Jose: highest totalPayAndBenefits for job " + jobTitlePO);
        Dataset<SalaryRecord> richestPoliceSJ = salaryDSSJ17.filter(salaryDSSJ17.col("jobTitle").equalTo(jobTitlePO)).orderBy(col("totalPayAndBenefits").desc());
        richestPoliceSJ.show(1);
        
        //TODO 9: In 2017 San Francisco data set, find the highest totalPayAndBenefits for the jobTitle "Police Officer"
        System.out.println("---2017 San Francisco: highest totalPayAndBenefits for job " + jobTitlePO);
        Dataset<SalaryRecord> richestPoliceSF = salaryDSSF17.filter(salaryDSSF17.col("jobTitle").equalTo(jobTitlePO)).orderBy(col("totalPayAndBenefits").desc());
        richestPoliceSF.show(1);
        
        //TODO 10: print the jobTitle with best average totalPayAndBenefits in 2017 San Jose data set
        System.out.println("---2017 San Jose: the job with highest avgerate TotalPayAndBenefits");
        Dataset<Row> highestDS = salaryDSSJ17.groupBy("jobTitle").avg("totalPayAndBenefits").orderBy(col("avg(totalPayAndBenefits)").desc());
        highestDS = highestDS.withColumnRenamed("avg(totalPayAndBenefits)", "avgTotalPayAndBenefitsSJ17");
        highestDS.show(1);
        //highestDS.select("jobTitle", "avgTotalPayAndBenefitsSJ17").show(1);
        
        
        //TODO 11: print the jobTitle with biggest difference between max and min totalPayAndBenefits in 2017 San Jose data set, and the difference
        System.out.println("---2017 San Jose: the 2 jobs with biggest difference between max and min TotalPayAndBenefits");
        Dataset<Row> min = salaryDSSJ17.groupBy("jobTitle").min("totalPayAndBenefits");
        Dataset<Row> max = salaryDSSJ17.groupBy("jobTitle").max("totalPayAndBenefits");
        
        Dataset<Row> valMin = min.withColumnRenamed("min(totalPayAndBenefits)", "minTotalPayAndBenefitsSJ17");
        Dataset<Row> valMax = max.withColumnRenamed("max(totalPayAndBenefits)", "maxTotalPayAndBenefitsSJ17");

        Dataset<Row> combine = valMax.join(valMin, "jobTitle"); 
        combine = combine.withColumn("diffMaxMinTotalPayAndBenefits", combine.col("maxTotalPayAndBenefitsSJ17").minus(combine.col("minTotalPayAndBenefitsSJ17")));
        combine.orderBy(col("diffMaxMinTotalPayAndBenefits").desc()).show(2);
      

        //TODO 12: In 2017 San Jose data set, find the jobTitle with highest average totalPayAndBenefits increase from 2016
        System.out.println("---2016-2017 San Jose: the 2 jobs with biggest increase of avg TotalPayAndBenefits");
        Dataset<Row> sj17 = salaryDSSJ17.groupBy("jobTitle").avg("totalPayAndBenefits");
        Dataset<Row> sj16 = salaryDSSJ16.groupBy("jobTitle").avg("totalPayAndBenefits");
        sj17 = sj17.withColumnRenamed("avg(totalPayAndBenefits)", "avgTotalPayAndBenefitsSJ17");
        sj16 = sj16.withColumnRenamed("avg(totalPayAndBenefits)", "avgTotalPayAndBenefitsSJ16");
        
        Dataset<Row> combined2 = sj16.join(sj17, "jobTitle");
        combined2 = combined2.withColumn("diff1617TotalPayAndBenefits", combined2.col("avgTotalPayAndBenefitsSJ17").minus(combined2.col("avgTotalPayAndBenefitsSJ16")));
        combined2.orderBy(col("diff1617TotalPayAndBenefits").desc()).show(2);
        
        
        spark.stop();

    }
}
