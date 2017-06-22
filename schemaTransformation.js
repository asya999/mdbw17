/* schema transformations */
/* sample documents */
db.vmessages.insert([
{ "_id" : ObjectId("5940523ba28125ddb15f15d2"), "body": {"VMESSAGE": {"072ade7d42d8" : { "messageId" : "072ade7d42d8", "create" : 1486546629585, "status" : "accept", "comment": "hi" } },
{ "_id" : ObjectId("5940523ba28125ddb15f15d3"), "body": {"VMESSAGE": {"595d0a56cff2" : { "messageId" : "595d0a56cff2", "create" : 1486566646197, "status" : "reject", "comment": "no good" } },
{ "_id" : ObjectId("5940523ba28125ddb15f15d4"), "body": {"VMESSAGE": {"52ffd09bf5b5" : { "messageId" : "52ffd09bf5b5", "create" : 1486568943752, "status" : "accept" } }
]);
/* desired results */
result=[
{ "_id" : ObjectId("5940523ba28125ddb15f15d2"), "message" : { "vid":"072ade7d42d8", "messageId" : "072ade7d42d8", "create" : 1486546629585, "status" : "accept", "comment": "hi" } },
{ "_id" : ObjectId("5940523ba28125ddb15f15d3"), "message" : { "vid":"595d0a56cff2", "messageId" : "595d0a56cff2", "create" : 1486566646197, "status" : "reject", "comment": "no good" } },
{ "_id" : ObjectId("5940523ba28125ddb15f15d4"), "message" : { "vid":"52ffd09bf5b5", "messageId" : "52ffd09bf5b5", "create" : 1486568943752, "status" : "accept" } }
];

/* aggregations */
/* aggregation with many addFields stages */
db.vmessages.aggregate([
    {"$addFields": {
        "bvm": {"$objectToArray":"$body.VMESSAGE"}
    } },
    {"$addFields": {
        "bvm2": {"$arrayElemAt":["$bvm",0]}
    } },
    {"$addFields": {
        "bvm": {"$objectToArray":"$bvm2.v"}
    } },
    {"$addFields": {
        "msgarr": {
           "$concatArrays": [
               [ { "k":"vid", "v":"$bvm2.k"} ],
               "$bvm"
           ]
        }
    } },
    {"$addFields": {
         "message": {"$arrayToObject": "$msgarr"}
    } }
]);

/* aggregation with a single addFields or project stage */
db.vmessages.aggregate([
  {"$addFields":{
      "message":{
          "$arrayToObject":{
               "$let":{
                   "vars":{
                        "elem": {"$arrayElemAt":[
                                {"$objectToArray":"$body.VMESSAGE"},
                                0
                        ]}
                    },
                    "in":{"$concatArrays":[ 
                          [ { k: "vid", v: "$$elem.k" } ],
                          {"$objectToArray": "$$elem.v"} 
                     ]}
               }
          }
      }
  }}
]);

