import org.warcbase.spark.matchbox._ 
import org.warcbase.spark.rdd.RecordRDD._ 
import org.warcbase.spark.matchbox.ExtractEntities
import org.warcbase.spark.utils.JsonUtil
import scala.collection.mutable.MutableList

sc.addFile("/home/ubuntu/warcbase-resources/NER/english.all.3class.distsim.crf.ser.gz")

def combineKeyCountLists (l1: List[(String, Int)], l2: List[(String, Int)]): List[(String, Int)] = {
    (l1 ++ l2).groupBy(_._1 ).map {
      case (key, tuples) => (key, tuples.map( _._2).sum) 
    }.toList
  }


  class Entity(iEntity: String, iFreq: Int) {
    var entity: String = iEntity
    var freq: Int = iFreq
  }

  class EntityCounts(iNerType: String) { 
    var nerType: String = iNerType
    var entities = MutableList[Entity]()
  }

  class NerRecord(recDate: String, recDomain: String) {
    var date = recDate
    var domain = recDomain

    var ner = MutableList[EntityCounts]()
  }

val rdd = RecordLoader.loadArchives("/mnt/olympics/ARCHIVEIT-7227-CRAWL_SELECTED_SEEDS-JOB237586-20160924051457128-00076.warc.gz", sc).
    keepValidPages().
    map(r => (r.getCrawlDate, r.getUrl, RemoveHTML(r.getContentString)))

val iNerClassifierFile = "/home/ubuntu/warcbase-resources/NER/english.all.3class.distsim.crf.ser.gz"
val outputFile = "ner3"

      rdd.mapPartitions(iter => {
        NER3Classifier.apply(iNerClassifierFile)
          iter.map(r => {
            val classifiedJson = NER3Classifier.classify(r._3)
            val classifiedMap = JsonUtil.fromJson(classifiedJson)
            val classifiedMapCountTuples: Map[String, List[(String, Int)]] = classifiedMap.map {
              case (nerType, entities: List[String @unchecked]) => (nerType, entities.groupBy(identity).mapValues(_.size).toList)
            }
            ((r._1, r._2), classifiedMapCountTuples)
          })
      }).reduceByKey( (a, b) => (a ++ b).keySet.map(r => (r, combineKeyCountLists(a(r), b(r)))).toMap).mapPartitions(iter => {
        iter.map(r => {
          val nerRec = new NerRecord(r._1._1, r._1._2)
          r._2.foreach(entityMap => {  
            // e.g., entityMap = "PERSON" -> List(("Jack", 1), ("Diane", 3))
            val ec = new EntityCounts(entityMap._1)    
            entityMap._2.foreach(e => {
              ec.entities += new Entity(e._1, e._2)
            })
            nerRec.ner += ec
          })
          JsonUtil.toJson(nerRec)
        })
      }).saveAsTextFile(outputFile)
