var express = require('express');
var request = require('request');
var cassandra = require('cassandra-driver');

var client = new cassandra.Client({ contactPoints: ['127.0.0.1:9042'], keyspace: 'test'});

var app = express()


app.get('/', function(req, res) {
  res.send('Welcome to our database seeder');
})

app.get('/updateDatabase', function (req, res) {
  var crimeData;
  request('https://data.cityoftacoma.org/resource/vzsr-722t.json', function(error, response, data){
    if (!error && response.statusCode == 200) {
      crimeData = data;
      var crimes = {};
      parsedCrimeData = JSON.parse(crimeData);
      //console.log(parsedCrimeData)
      for (var i = 0; i < parsedCrimeData.length; i++) {
        if(parsedCrimeData[i].intersection != null) {
          crimes["Crime " + i]= parsedCrimeData[i];
        }
      }
      console.log(crimes);
      var cleanCrimeData = JSON.stringify(crimes);
      for (var crime in cleanCrimeData) {
        var query = 'INSERT INTO crimeData (incident_number, crime, date, intersection, time) Values (?,?,?,?,?)';
        var params = [ '1', crime.crime, crime.date, crime.intersection, crime.occured_on];
      }

      console.log('Attempting to seed db')

      client.execute(query, params, { prepare: true }, function(error) {
        if (error) {
          console.log(error)
        } else {
          console.log('The database has been updated.')
        }
      })

    }
  })
  res.send('The database has been updated. :)')
})

app.listen(3000, function (){
  console.log('Example listening on port 3000')
})
