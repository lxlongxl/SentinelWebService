var express = require('express');
var request = require('request');
var cassandra = require('cassandra-driver');
var _ = require('underscore')

var client = new cassandra.Client({ contactPoints: ['127.0.0.1:9042'], keyspace: 'test'});
client.connect(function (error) {
  if (error) {
    console.log(error);
  }
});

var app = express()


app.get('/', function(req, res) {
  res.send('Welcome to our database seeder');
})

app.get('/updateDatabase', function (req, res) {
  var crimeData;
  request('https://data.cityoftacoma.org/resource/vzsr-722t.json', function(error, response, data){
    if (!error && response.statusCode == 200) {
      crimeData = data;
      var crimeJson = {};
      var crimes = [];
      crimeJson.crimes = crimes;
      console.log(crimeJson);
      console.log('Parsing to find complete data');
      parsedCrimeData = JSON.parse(crimeData);
      //console.log(parsedCrimeData)
      for (var i = 0; i < parsedCrimeData.length; i++) {
        if(parsedCrimeData[i].intersection != null) {
          crimeJson.crimes.push(parsedCrimeData[i]);
        }
      }

      const truncateQuery = 'TRUNCATE TABLE CRIMEDATA;'
      client.execute(truncateQuery, function(error) {
        if (error) {
          console.log(error);
        } else {
          console.log('Table truncated successfully');
        }
      });

      _.each(crimeJson.crimes, function(crime){
        console.log("Attempting insert entry into database");
        const query = 'INSERT INTO test.crimeData (incident_number, crime, date, intersection, intersection_address, intersection_city, intersection_state, time) Values (?,?,?,?,?,?,?,?);';
        const params = [crime.incident_number, crime.crime, crime.occurred_on, crime.intersection.coordinates, crime.intersection_address, crime.intersection_city, crime.intersection_state, crime.approximate_time];

        client.execute(query, params, { prepare: true }, function(error) {
          if (error) {
            console.log(error);
          } else {
            console.log('The database has been updated.');
          }
        });
      });
    } else {
      console.log(error);
    }
  });

  res.send('The database has been updated. :)');
})

app.listen(3000, function (){
  console.log('Example listening on port 3000');
})
