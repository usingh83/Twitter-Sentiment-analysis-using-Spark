package lab

import org.apache.spark.Logging
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Streaming extends Logging {
  def setStreamingLogLevels(){
    val log4jInitialized=Logger.getRootLogger.getAllAppenders.hasMoreElements()
    if(log4jInitialized){
      logInfo("Set to [WARN]")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
    }
 }