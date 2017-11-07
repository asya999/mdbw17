/* Find logins from different IPs within 10 minutes over specified time period */
/* sample documents */
db.logins.insert([
{ "_id" : ObjectId("5913ea37c76b4f4eb313bd8b"), "user" : "247751", "ipaddr" : "12.130.117.22", "ts" : ISODate("2017-05-08T04:18:16Z") },
{ "_id" : ObjectId("5913ea37c76b4f4eb313bd8c"), "user" : "247547", "ipaddr" : "71.84.12.168", "ts" : ISODate("2017-05-08T04:18:27Z") },
{ "_id" : ObjectId("5913ea37c76b4f4eb313bd8d"), "user" : "301988", "ipaddr" : "12.130.117.87", "ts" : ISODate("2017-05-08T04:18:31Z") },
{ "_id" : ObjectId("5913ea37c76b4f4eb313bd8e"), "user" : "303900", "ipaddr" : "71.56.112.56", "ts" : ISODate("2017-05-08T04:18:35Z") },
{ "_id" : ObjectId("5913ea37c76b4f4eb313bd8f"), "user" : "301988", "ipaddr" : "12.130.117.87", "ts" : ISODate("2017-05-08T04:16:02Z") },
{ "_id" : ObjectId("5913ea37c76b4f4eb313bd90"), "user" : "303900", "ipaddr" : "71.56.112.56", "ts" : ISODate("2017-05-08T04:16:31Z") },
{ "_id" : ObjectId("5913ea37c76b4f4eb313bd91"), "user" : "249609", "ipaddr" : "172.21.133.121", "ts" : ISODate("2017-05-08T04:16:39Z") },
{ "_id" : ObjectId("5913ea37c76b4f4eb313bd92"), "user" : "249609", "ipaddr" : "172.21.133.121", "ts" : ISODate("2017-05-08T04:16:47Z") }
]);
db.logins.createIndex({ts:1});
var start=ISODate("2017-05-01T00:00:00Z") /* fill in date here */
var end=ISODate("2017-05-15T00:00:00Z") /* fill in date here */
/* aggregation 1 */
db.logins.aggregate([
{$match:{ts:{$gte:start,$lt:end}}},
{$sort:{ts:1}},
{$group:{_id:"$user",ips:{$push:{ip:"$ipaddr", ts:"$ts"}}}},
{$addFields:{diffs: {$filter:{
    input:{$map:{
          input: {$range:[0,{$subtract:[{$size:"$ips"},1]}]}, as:"i",
              in:{$let:{vars:{ip1:{$arrayElemAt:["$ips","$$i"]},
                              ip2:{$arrayElemAt:["$ips",{$add:["$$i",1]}]}},
                        in:{
                            diff:{$divide:[{$abs:{$subtract:["$$ip1.ts","$$ip2.ts"]}},60000]},
                            ip1:"$$ip1.ip", t1:"$$ip1.ts",
                            ip2:"$$ip2.ip", t2:"$$ip2.ts"
          }}}
    }},
    cond:{$and:[{$lt:["$$this.diff",10]},{$ne:["$$this.ip1","$$this.ip2"]}]}
}}}},
{$match:{"diffs":{$ne:[]}}},
{$project:{_id:0, user:"$_id", suspectLogins:"$diffs"}}
]);

/* aggregation 2 with extra stage to filter all from same IP */
db.logins.aggregate([
{$match:{ts:{$gte:start,$lt:end}}},
{$sort:{ts:1}},
{$group:{_id:"$user",ips:{$push:{ip:"$ipaddr", ts:"$ts"}},
                              diffIps:{$addToSet:"$ipaddr"}}},
{$match:{"diffIps.1":{$exists:true}}},
{$addFields:{diffs: {$filter:{
    input:{$map:{
          input: {$range:[0,{$subtract:[{$size:"$ips"},1]}]}, as:"i",
              in:{$let:{vars:{ip1:{$arrayElemAt:["$ips","$$i"]},
                              ip2:{$arrayElemAt:["$ips",{$add:["$$i",1]}]}},
                        in:{
                            diff:{$divide:[{$abs:{$subtract:["$$ip1.ts","$$ip2.ts"]}},60000]},
                            ip1:"$$ip1.ip", t1:"$$ip1.ts",
                            ip2:"$$ip2.ip", t2:"$$ip2.ts"
          }}}
    }},
    cond:{$and:[{$lt:["$$this.diff",10]},{$ne:["$$this.ip1","$$this.ip2"]}]}
}}}},
{$match:{"diffs":{$ne:[]}}},
{$project:{_id:0, user:"$_id", suspectLogins:"$diffs"}}
]);

/* aggregation 3 with reducing work in the diff computation stage */
db.logins.aggregate([
{$match:{ts:{$gte:start,$lt:end}}},
{$sort:{ts:1}},
{$group:{_id:"$user",ips:{$push:{ip:"$ipaddr", ts:"$ts"}},
                            diffIps:{$addToSet:"$ipaddr"}}},
{$match:{"diffIps.1":{$exists:true}}},
{$addFields:{diffs: {$filter:{
  input:{$map:{
    input: {$range:[0,{$subtract:[{$size:"$ips"},1]}]}, as:"i",
    in:{$let:{vars:{ip1:{$arrayElemAt:["$ips","$$i"]},
                    ip2:{$arrayElemAt:["$ips",{$add:["$$i",1]}]}},
      in:{
        diff:{$cond:{
               if:{$ne:["$$this.ip1","$$this.ip2"]},
               then:{$divide:[{$subtract:["$$ip2.ts","$$ip1.ts"]},60000]}]},
               else: 9999 }},
        ip1:"$$ip1.ip", t1:"$$ip1.ts",
        ip2:"$$ip2.ip", t2:"$$ip2.ts"
   }}}}},
   cond:{$lt:["$$this.diff",10]}
}}}},
{$match:{"diffs":{$ne:[]}}},
{$project:{_id:0, user:"$_id", suspectLogins:"$diffs"}}
]);

