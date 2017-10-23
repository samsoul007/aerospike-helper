const AS = require("./index")

/**
 * Create a client based on a host
 * @param {string} name optional
 * @param {string} host ip:port
 */
AS.AddClient(["52.48.8.4:3000"]);

var q = AS.Query("test")

AS.connect()
  .then(function(response) {
    return q.write("test2", {
      bin1: "test45",
      bin2: "test2",
      bin3: "test3",
      bin5: 3
    })
  })
  .then(function(response) {
    return q.write("test3", {
      bin1: "test32",
      bin2: "test2",
      bin3: "test5",
      bin5: 20,
      bin7: {
        test:1234
      },
      loc: AS.GeoPoint(12.5683,55.6761) //lon lat
    })
  })
  .then(function(response) {
    return q.write("test4", {
      bin1: ["test4","test23","test56"],
      bin2: "test2",
      bin3: "test3",
      bin5: 10,
      bin6: ["test4","test23","test56"]
    })
  })
  .then(function(response) {
    return q.write("test5", {
      bin1: "test4",
      bin2: "test2",
      bin3: "test3",
      bin5: 5
    })
  })
  // .then(function() {
  //   return q.read("test555","test","test2")
  // })
  // .then(function(){
  //   return q.removeIndex("loc", "GEO")
  // })
  .then(function() {
    return q.createIndex("loc", "GEO")
  })
  .then(function() {
    return q.createIndex("bin3", "STR")
  })
  .then(function() {
    return q.must(
      // index lon lat radius in m
      AS.filter.geo.radius("loc",12.5741441,55.6118459,9000),
      AS.filter.equal("bin3", "test6")
      // AS.filter.contains("bin6", "test4"),
      // AS.filter.range("bin7.test", 0, 100000)
    );
  })
  .then(function(recs) {
    console.log("recs", recs)
    process.exit();
  })
  .catch(function(err) {
    console.log(err)
    process.exit();
  })
