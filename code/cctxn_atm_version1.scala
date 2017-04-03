
// version1

val input_file_path = "/home/cloudera/Desktop/atm/*.csv"
val load_atm=sc.textFile(input_file_path)
case class ATM(actor_type:String,actor_id:String,action_type:String,action_time:Long,object_type:String,object_id:String,channel_type:String,channel_id:String,partition_time:Long,theme:String)
val atm_rdd=load_atm map{l=>val p=l.replace("\\","").split(",")
						    val pre_fields=p.slice(0,8)
                            val post_fields=p.slice(p.length-2,p.length)
                            ATM(pre_fields(0),pre_fields(1),pre_fields(2),pre_fields(3).toLong,pre_fields(4),pre_fields(5),pre_fields(6),pre_fields(7),post_fields(0).toLong,post_fields(1))
                         }
val atmDF=atm_rdd.toDF
atmDF.select($"attrs").rdd.map{r=>JSON.parseFull(r.toString)}.take(1)

val atm_json_rdd=load_atm map{l=>val p=l.replace("\\","").split(",")
	                             val attrs_origin=p.slice(8,p.length-2).mkString(",")
	                             attrs_origin.slice(1,attrs_origin.length-1)
                              }
val atm_jsonDF=spark.read.json(atm_json_rdd)

atm_jsonDF.take(1)