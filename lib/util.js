
module.exports = {
  uniqArray: function(array)
  {
    // #refactor:50 pull out unique code
    // getonly unique results
    debugger;
    var list = [];
    var unique = {};
    for (var i in array){
      if ( typeof(unique[array[i]]) == "undefined"){
        //console.log(array[i]);
        list.push(array[i]);
      }
      unique[array[i]] = 0;
    }
    return list;

  },

// #refactor:20 turn into streaming thing

/*
    // Querythe database for list to scan
    collection.find({screen_name: seed},
      {id: 1, screen_name: 1, followers: 1, following: 1}
    ).toArray(function(err, docs_){
      if (err) {
        console.log(err);
        return;
      }//
      console.log("found " + docs_.length + " docs");
      docs = docs_.map( function(doc){
        return {
          id: doc.id,
          screen_name: doc.screen_name || null,
          followers: doc.followers || null,
          following: doc.following || null
        };
      });//

      // #refactor:20 validate data from mongo
      docs.push.apply(docs_[0].followers.map(function(doc){
        return {
          id: doc.id,
          screen_name: doc.screen_name || null,
          followers: doc.followers || null,
          following: doc.following || null
        };
      }));
      docs.push.apply(docs_[0].following.map(function(doc){
        return {
          id: doc.id,
          screen_name: doc.screen_name || null,
          followers: doc.followers || null,
          following: doc.following || null
        };
      }));


      Process();

    }); // collection
*/
};
