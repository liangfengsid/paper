import org.apache.spark.{Partitioner,HashPartitioner}
import org.apache.spark.{SparkContext,SparkConf}
import java.util.StringTokenizer
import scala.collection.mutable.ListBuffer

class PrefixHashPartitioner(partitions: Int, prefix: Int) extends Partitioner {
  def numPartitions: Int = partitions
  def prefixLength: Int=prefix

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def getPartition(key: Any): Int = key match {
    case null => 0
    case word:String => {
    	val length=if (word.length>prefixLength){
    		prefixLength
    	}else{
    		word.length
    	}
    	val pre=word.substring(0,length)
    	nonNegativeMod(pre.hashCode, numPartitions)
    }
    case _ => 0
  }

  override def equals(other: Any): Boolean = other match {
    case h: PrefixHashPartitioner =>{
      h.numPartitions == numPartitions
      h.prefixLength == prefixLength
    }
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

object PrefixCount{
	def main(args:Array[String]){
        if (args.length == 0){
                System.err.println("Usage: PrefixCount <max> <min> <inputPath> <outputPath> <isPartitioned>")
                System.exit(1)
        }
        
        val maxLength=args(0).toInt
        val minLength=args(1).toInt
        val file=args(2)
        val outputPath=args(3)
        val isPartitioned=args(4).toBoolean
        
        /**
        val file="/wiki200/file_7"
        val maxLength=5
        val minLength=2
        val outputPath="/prefixOut"
        val isPartitioned=false
        */

        val conf=new SparkConf().setAppName("MatrixMultiplication")
        val sc=new SparkContext(conf)

        val input=sc.textFile(file)
        
        val indexPartitioner=if(isPartitioned){
        	new PrefixHashPartitioner(16,minLength)
        }else{
        	new HashPartitioner(16)
        }
        
        //count the prefix of the max length from the raw input file
        var preCountPair=input.flatMap(entry => {
        	val token=new StringTokenizer(entry)
        	val buffer=new ListBuffer[(String,Int)]()
        	while(token.hasMoreTokens()){
        		val word=token.nextToken();
        		val pre=if(word.length>maxLength){
        			word.substring(0,maxLength)
        		}else{
        			word
        		}
        		buffer+=((pre,1))
        	}
        	buffer.toList
        }).reduceByKey(indexPartitioner,(a,b) => a+b)
        preCountPair.saveAsTextFile(outputPath)
        
        //count the prefix of the descending-order length from the previous result of the previous iteration
		for{length<-maxLength-1 to (minLength,-1)}{
			//var length=3
			val newOutputPath=outputPath+length
				preCountPair=preCountPair.map(entry => {
				val prefix=entry._1
				val count=entry._2
				val pre=if(prefix.length>length){
					prefix.substring(0,length)
				}else{
					prefix
				}
				(pre,count)
			}).reduceByKey(indexPartitioner,(a,b) => a+b)
			preCountPair.saveAsTextFile(newOutputPath) 	
		}
	}
}
