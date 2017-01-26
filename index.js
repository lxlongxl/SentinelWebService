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
      var query = 'INSERT INTO crimeData (incident_number, crime, date, intersection, time) Values (?,?,?,?,?)';
      var params = [ '1', 'turtle head', '2017-01-26', 'hello', '12:00'];

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
