// version2

import scala.util.parsing.json._

// 先單純用字串測試
// val a = "{\"action\": {\"txn_amt\": 937377.4782018902, \"txn_fee_amt\": 0.0, \"trans_type\": \"trans_out\"}, \"object\": {\"target_bank_code\": \"013\", \"target_acct_nbr\": null}, \"channel\": {\"address_zipcode\": 165243, \"machine_bank_code\": \"013\"}}"
// val test =JSON.parseFull(a)
// test.get.asInstanceOf[Map[String,Any]].get("action").get.asInstanceOf[Map[String,Any]].get("txn_amt")

val input_file_path="/home/cloudera/Desktop/atm/*.csv"
val load_atm=sc.textFile(input_file_path)
case class ATM(actor_type:String,actor_id:String,action_type:String,action_time:Long,object_type:String,object_id:String,channel_type:String,channel_id:String,attrs:String,partition_time:Long,theme:String)
val atm_rdd=load_atm map{l=>val p=l.replace("\\","").split(",")
                            val pre_fields=p.slice(0,8)
                            val attrs_origin=p.slice(8,p.length-2).mkString(",")
                            val attrs = attrs_origin.slice(1,attrs_origin.length-1)
                            val post_fields=p.slice(p.length-2,p.length)
                            // JSON.parseFull(attrs.toString)
                            ATM(pre_fields(0),pre_fields(1),pre_fields(2),pre_fields(3).toLong,pre_fields(4),pre_fields(5),pre_fields(6),pre_fields(7),attrs.toString,post_fields(0).toLong,post_fields(1))
                        }
val atmDF=atm_rdd.toDF
atmDF.select($"attrs").rdd.map{r=>JSON.parseFull(r.toString)}.take(1)

// Question:用RDD做了以後就會多了一層List，該如何進入這層List將最裡面那層Map取出??
// Array[Option[Any]] = Array(Some(List(Map(action -> Map(txn_amt -> 937377.4782018902, txn_fee_amt -> 0.0, trans_type -> trans_out), object -> Map(target_bank_code -> 013, target_acct_nbr -> null), channel -> Map(address_zipcode -> 165243.0, machine_bank_code -> 013)))))


