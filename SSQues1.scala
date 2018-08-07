
package lab
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger}
import com.cybozu.labs.langdetect.DetectorFactory
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.elasticsearch.spark._
import scala.collection.Seq
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.Authorization
import twitter4j.TwitterFactory
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel._
import scala.util.Try
import lab.Geo._
import lab.Sentiment._

object SSQues1 {
  def main(args: Array[String]) {

    Streaming.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("SSQues1").setMaster("local[2]")
    .set(ConfigurationOptions.ES_PORT,"9200")
    .set(ConfigurationOptions.ES_NODES,"localhost")
    .set(ConfigurationOptions.ES_INDEX_AUTO_CREATE_DEFAULT, "true")
    
    val sc=new SparkContext(sparkConf)
    val ssc=new StreamingContext(sc,Seconds(2))
    
    val twitterconf=new twitter4j.conf.ConfigurationBuilder()
    .setOAuthAccessToken("825549110-X03gOO4URLJDkbAtwgEoKIzBH68bmhJ0XXXXCgCz")
    .setOAuthAccessTokenSecret("1cyClbpXXXruqWXIkPJHW8M5qtcUc5dTHUVASaQ")
    .setOAuthConsumerKey("dcabXXXpjegh6FEuQ3DTdQ")
    .setOAuthConsumerSecret("lPAOq2Ax5s28tteLp0pX8qKPy3oPEW8g20xvXXXJ0So").build()
         
    val factory=new TwitterFactory(twitterconf)
    val auth=new twitter4j.auth.OAuthAuthorization(twitterconf)
    val twitterAuth: Option[twitter4j.auth.Authorization]=Some(factory.getInstance(auth).getAuthorization())
    val filters=Seq("#Trump")
    val storageLevel= MEMORY_AND_DISK
    
    val stream = TwitterUtils.createStream(ssc, twitterAuth, filters, storageLevel)
    stream.map(x=>x.getText).print()
    stream.foreachRDD{(rdd, time)=>{
      rdd.filter(t=> t.getUser.getLocation!=None)
    }}
    /*stream.foreachRDD{(rdd, time) =>
       rdd.map(t => {
         Map(
           "user"-> t.getUser.getScreenName,
           "created_at" -> t.getCreatedAt.toInstant.toString,
           "location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" }),
           "text" -> t.getText,
           "hashtags" -> t.getHashtagEntities.map(_.getText),
           "retweet" -> t.getRetweetCount,
           "language" -> detectLanguage(t.getText),
           "sentiment" -> detectSentiment(t.getText).toString
         )
       }).saveToEs("twitter/tweet")
		}*/
    
    stream.foreachRDD{(rdd, time) =>
       rdd.map(t => {
         Map(
           "user"-> t.getUser.getName,
           "created_at" -> t.getCreatedAt.toInstant.toString,
           "location" -> Option(getLatLon(t.getUser.getLocation)).map(geo => { s"${geo.latitude},${geo.longitude}" }),
           "text" -> t.getText,
           "hashtags" -> t.getHashtagEntities.map(_.getText),
           "retweet" -> t.getRetweetCount,
           //"language" -> t.getLang,
           "place" -> t.getUser.getLocation,
           "language" -> detectLanguage(t.getText),
           "sentiment" -> detectSentiment(t.getText).toString
         )
       }).saveToEs("twitter/tweet")}
    /*val words = stream.flatMap(status => status.getText.split(" ")).filter(word=>word.matches("[a-zA-Z]+"))

    val res = words.map((_, 1)).reduceByKey(_ + _)
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))

    res.foreachRDD(rdd => {
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      rdd.collect().foreach{case (count, tag) => println("(%s ,%s )".format(tag,count))}
      })
      * 
      */
    ssc.start()
    ssc.awaitTermination()
  }
  def detectLanguage(text: String) : String = {

    Try {
      val detector = DetectorFactory.create()
      detector.append(text)
      detector.detect()
    }.getOrElse("unknown")

}
}