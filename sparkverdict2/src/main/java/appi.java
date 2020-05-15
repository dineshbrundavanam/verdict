import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.verdictdb.VerdictContext;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.exception.VerdictDBException;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class appi implements Serializable {

    public static void main(String[] args) throws InterruptedException, VerdictDBException {

        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();

        /*SparkSession spark = SparkSession.builder().appName("Simple Application").config("spark.master","local").getOrCreate();*/
        SparkSession spark = SparkSession.builder().appName("Simple Application").master("local[*]").getOrCreate();
        /*SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();*/

        spark.sparkContext().setLogLevel("ERROR");

        VerdictContext verdict = VerdictContext.fromSparkSession(spark);

        Dataset<Row> csv = spark.read().format("csv").option("header","true").load("/Users/dinesh/Desktop/50_Startups.csv");
        csv.show();
        csv.createOrReplaceTempView("startup");
        VerdictSingleResult vr = verdict.sql("show databases");
        VerdictSingleResult vr2 = verdict.sql("select count(*) from verdictdbtemp.startup");
        vr.printCsv();
        vr2.printCsv();




        spark.sql("DROP SCHEMA IF EXISTS myschema CASCADE");
        spark.sql("CREATE SCHEMA IF NOT EXISTS myschema");
        spark.sql("CREATE TABLE IF NOT EXISTS myschema.sales (product string, price double)");
        spark.sql("INSERT INTO myschema.sales VALUES ('Cat', 12.35)");
        spark.sql("INSERT INTO myschema.sales VALUES ('Dog', 33.12)");
        spark.sql("INSERT INTO myschema.sales VALUES ('Fish', 8.26)");

        /*spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");*/

        verdict.sql("BYPASS DROP TABLE IF EXISTS myschema.sales_scramble");
        verdict.sql("BYPASS DROP SCHEMA IF EXISTS verdictdbtemp CASCADE");
        verdict.sql("BYPASS DROP SCHEMA IF EXISTS verdictdbmeta CASCADE");
        verdict.sql("CREATE SCRAMBLE myschema.sales_scramble FROM myschema.sales BLOCKSIZE 100");

        VerdictSingleResult vrd = verdict.sql("select count(*) from myschema.sales");
        vrd.printCsv();


        /*List<Row> list=new ArrayList<Row>();
        list.add(RowFactory.create("one"));
        list.add(RowFactory.create("two"));
        list.add(RowFactory.create("three"));
        list.add(RowFactory.create("four"));
        List<org.apache.spark.sql.types.StructField> listOfStructField=new ArrayList<org.apache.spark.sql.types.StructField>();
        listOfStructField.add(DataTypes.createStructField("test", DataTypes.StringType, true));
        StructType structType=DataTypes.createStructType(listOfStructField);
        Dataset<Row> data=spark.createDataFrame(list,structType);
        data.show();

        Dataset<Row> csv = spark.read().format("csv").option("header","true").load("/Users/dinesh/Desktop/50_Startups.csv");
        csv.show();
        csv.select("Profit").show();
        csv.createOrReplaceTempView("startup");
        Dataset<Row> sqlDF = spark.sql("SELECT sum(Profit), State FROM startup group by State");
        sqlDF.show();*/


        long numAs = 10;
        long numBs = 20;

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
        Thread.sleep(1000000);
        spark.stop();
    }
}
