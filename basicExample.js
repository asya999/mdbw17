/* basic books example */
/* sample documents */
db.books.insert([
{ "_id" : ObjectId("59457a75d975a00a7119a96d"), "title" : "The Great Gatsby", "language" : "English", "subjects" : [ "Long Island", "New York", "1920s" ] },
{ "_id" : ObjectId("59457a75d975a00a7119a96e"), "title" : "War and Peace", "language" : "Russian", "subjects" : [ "Russia", "War of 1812", "Napoleon" ] },
{ "_id" : ObjectId("59457a75d975a00a7119a96f"), "title" : "Open City", "language" : "English", "subjects" : [ "New York", "Harlem" ] }
]);
db.books.createIndex({language:1});
/* aggregation */
db.books.aggregate([
    {"$match":{"language":"English"}},
    {$unwind:"$subjects"},
    {$group:{_id:"$subjects",count:{$sum:1}}},
    {$sort:{count:-1}},
    {$limit:3},
    {$project:{_id:0,subject:"$_id", count:"$count"}}
]);
/* different aggregations with explain */
db.books.aggregate([
    {"$match":{"language":"English"}},
    {$unwind:"$subjects"},
    {$group:{_id:"$subjects",count:{$sum:1}}},
    {$sort:{count:-1}},
    {$limit:3},
    {$project:{_id:0,subject:"$_id", count:"$count"}}
], {explain:true});
db.books.aggregate([
    {$unwind:"$subjects"},
    {$group:{_id:"$subjects",count:{$sum:1}}},
    {$sort:{count:-1}},
    {$limit:3},
    {$project:{_id:0,subject:"$_id", count:"$count"}}
], {explain:true});
db.books.aggregate([
    {$unwind:"$subjects"},
    {"$match":{"language":"English"}},
    {$group:{_id:"$subjects",count:{$sum:1}}},
    {$sort:{count:-1}},
    {$limit:3},
    {$project:{_id:0,subject:"$_id", count:"$count"}}
], {explain:true});
db.books.aggregate([
    {$unwind:"$subjects"},
    {"$match":{"language":"English","subjects":/^[ABC]/}},
    {$group:{_id:"$subjects",count:{$sum:1}}},
    {$sort:{count:-1}},
    {$limit:3},
    {$project:{_id:0,subject:"$_id", count:"$count"}}
], {explain:true});
