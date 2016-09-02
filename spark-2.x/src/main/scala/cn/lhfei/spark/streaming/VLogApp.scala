package cn.lhfei.spark.streaming

import java.text.SimpleDateFormat

import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import cn.lhfei.spark.df.streaming.vo.VLogger
import cn.lhfei.spark.df.streaming.vlog.VType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

object VLogApp {

  val SF: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  val NORMAL_DATE: String = "1976-01-01";

  case class VideologPair(key: String, value: String);

  object vlog_schema {
    val err		= StructField("err", StringType)
    val ip		= StructField("ip", StringType)
    val ref		= StructField("ref", StringType)
    val sid		= StructField("sid", StringType)
    val uid		= StructField("uid", StringType)
    val from	= StructField("from", StringType)
    val loc		= StructField("loc", StringType)
    val cat		= StructField("cat", StringType)
    val tm		= StructField("tm", StringType)
    val url		= StructField("url", StringType)
    val dur		= StructField("dur", StringType)
    val bt		= StructField("bt", StringType)
    val bl		= StructField("bl", StringType)
    val lt		= StructField("lt", StringType)
    val vid		= StructField("vid", StringType)
    val ptype	= StructField("ptype", StringType)
    val cdnId	= StructField("cdnId", StringType)
    val netname	= StructField("netname", StringType)
    val tr		= StructField("tr", StringType)

    val struct = StructType(Array(err, ip, ref, sid, uid, from, loc, cat, tm, url, dur, bt, bl, lt, vid, ptype, cdnId, netname, tr))
  }


  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .appName("Video Log Parser")
      .getOrCreate();
    
    
    val records = filteLog(spark);
    System.out.println(records.count());
    
    val vlDF = conver2Vo(spark);
    System.out.println("============" +vlDF.count())
    //vlDF.foreach { x => System.out.println("++++++++CAT: " +x.getCat) }
    
    groupBy(vlDF);
    
    toDataframe(vlDF, spark)

  }
  
  def toDataframe(vlDF: RDD[VLogger], spark:SparkSession): Unit = {
    var df = spark.createDataFrame(vlDF, Class.forName("cn.lhfei.spark.df.streaming.vo.VLogger"))
    
    df.createOrReplaceTempView("VL");
    
    df.show();
    
    val catDF = spark.sql("select cat as CAT, count(*) as Total from VL group by cat order by Total desc")
    
    catDF.show();
  }
  
  def groupBy(vlDF: RDD[VLogger]): Unit = {
    val keyMaps = vlDF.groupBy { x => x.getCat }
    keyMaps.foreach(f => System.out.println("Key: [" + f._1 + "], Total: [" + f._2.size + "]"))
  }

  def conver2Vo(spark: SparkSession): RDD[VLogger] = {
    val logFile = spark.sparkContext.textFile("/spark-data/vlogs/0001.txt");
    
    def convert(items: Array[String]): VLogger = {
      var vlog: VLogger  = new VLogger();
      
			var err:String = items(15);
			if (err != null && err.startsWith("301030_")) {
				err = "301030_X";
			}
			
			vlog.setErr(err);
			vlog.setIp(items(2));
			vlog.setRef(items(3));
			vlog.setSid(items(4));
			vlog.setUid(items(5));
			vlog.setLoc(items(8));
			vlog.setCat(VType.getVType(items(21), items(7), items(20)));
			
			vlog.setTm(items(11));
			
			vlog.setUrl(items(12));
			vlog.setDur(items(14));
			vlog.setBt(items(16));
			vlog.setBl(items(17));
			vlog.setLt(items(18));
			vlog.setVid(items(20));
			vlog.setPtype(items(21));
			vlog.setCdnId(items(22));
			vlog.setNetname(items(23));
			
			//System.out.println(">>>>>>>>>>>>Cat: "+vlog.getCat);
      
      return vlog;
    }
    
    return logFile.map[Array[String]] { x => x.split(" ") }.map[VLogger] { items => convert(items) }

  }

  def filteLog(spark: SparkSession): RDD[Array[String]] = {
    var ts: TimeStamp = null;
    var vlog: VLogger = null;
    val logFile = spark.sparkContext.textFile("/spark-data/vlogs/mini.txt");
    
    return logFile.map[Array[String]] { x => x.split(" ") }
  }
}