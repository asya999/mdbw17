/* example functions from talk */
/* $sum implemented via $reduce */
sumArray = function(inputArray) {
    return {"$reduce":{
        "input": inputArray,
        "initialValue": 0,
        "in":{"$add":["$$value","$$this"]}
    }};
}

/* $reverseArray implemented via $reduce */
reverseArray = function(inputArray) {
    return {"$reduce":{
        "input": inputArray,
        "intialValue":[],
        "in":{"$concatArrays":[
            [ "$$this" ],
            "$$value"
        ]}
    }};
};

/* sort the input array in descending order
 * 
 * inputArray - array to be sorted
 *              expecting literal string "$a" or 
 *              an expression resolving to array
 * sortField  - field to compare for ordering 
 *              if not set or "" then full array element is used
 *              so, if sorting on "$a.f" then sortField is "f"
 *              if sorting array of scalars then sortField is ""
 * asc        - if true sort in ascending order 
 *              just reverse when done
 */
sortArray = function( inputArray, sortField="", asc=false) {       
  var suffix = "";
  var maxF = MaxKey;
  var minF = MinKey;
  if (sortField != "") {
    suffix = "."+sortField;
    maxF = {}; minF = {};
    if (sortField.indexOf('.') == -1) {
       maxF[sortField] = MaxKey;
       minF[sortField] = MinKey;
    } else { 
       var mx = maxF;
       var mn = minF;
       var tokens = sortField.split('.');
       tokens.slice(0,tokens.length-1).forEach(function(m) {
           mx[m] = {}; mx = mx[m];
           mn[m] = {}; mn = mn[m];
       });
       mx[tokens[tokens.length-1]] = MaxKey;
       mn[tokens[tokens.length-1]] = MinKey;
    }
  }
  var initialArray = [maxF, minF];
  var reduce = {$reduce:{
      input:inputArray, 
      initialValue: initialArray,
      in: {$let:{ 
        vars: { rv:"$$value", rt:"$$this"},   
        in: {$let:{
          vars:{ 
            idx:{ $reduce:{ 
              input:{$range:[0,{$size:"$$rv"}]}, 
              initialValue: 9999999, 
              in: {$cond:[ 
                {$gt:["$$rt"+suffix, {$arrayElemAt:["$$rv"+suffix,"$$this"]}]}, 
                {$min:["$$value","$$this"]}, 
                "$$value" 
              ]}
            }}
          },
          in: {$concatArrays:[ 
            {$cond:[ 
              {$eq:[0, "$$idx"]}, 
              [ ],
              {$slice: ["$$rv", 0, "$$idx"]}
            ]},
            [ "$$rt" ], 
            {$slice: ["$$rv", "$$idx", {$size:"$$rv"}]} 
          ]} 
        }}
      }}
  }};
  var sz = {$size:inputArray};
  var sliceReduce = {$cond:[
      {$eq:[ 0, sz ]},
      [],
      {$slice:[
        reduce,
        1, sz 
      ]}
  ]};
  if (asc) return {$reverseArray:sliceReduce};
  else return sliceReduce;
}
