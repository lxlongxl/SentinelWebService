var express = require('express');
var app = express.Router();
var request = require('request');
var cassandra = require('cassandra-driver');
var _ = require('underscore');
var session = require('express-session');
var finalPayload = {};

var client = new cassandra.Client({ contactPoints: ['127.0.0.1:9042'], keyspace: 'test'});
client.connect(function (error) {
  if (error) {
    console.log(error);
  }
});

/* GET home page. */
app.get('/', function(req, res, next) {
  res.render('index', { title: 'SentinelWebService', message: "Welcome to the Project Sentinel Web Service!" });
});

app.get('/getCrimeData.json', function(req, res) {
  var crimePayload = [];
  finalPayload.crimePayload = crimePayload;
  client.execute('SELECT * FROM test.crimeData', function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.crimePayload.push(entry);
      })
      //Uncomment to verify is paylaod was generated
      //console.log(finalPayload);
      res.json(finalPayload);
    }
  });
});

app.get('/getCrimesPayload.json', function (req, res) {
  req.finalPayload = finalPayload;
  var simpleAssaultCrimes = [];
  finalPayload.simpleAssaultCrimes = simpleAssaultCrimes;
  const query = 'SELECT * FROM test.crimeData WHERE crime = ? ALLOW FILTERING;';
  client.execute(query, ['Simple Assault'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.simpleAssaultCrimes.push(entry);
      })
    }
  });

  var motorVehicleTheftCrimes = [];
  finalPayload.motorVehicleTheftCrimes = motorVehicleTheftCrimes;
  client.execute(query, ['Motor Vehicle Theft'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.motorVehicleTheftCrimes.push(entry);
      })
    }
  });

  var theftFromMotorVehicleCrimes = [];
  finalPayload.theftFromMotorVehicleCrimes = theftFromMotorVehicleCrimes;
  client.execute(query, ['Theft From Motor Vehicle'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.theftFromMotorVehicleCrimes.push(entry);
      })
    }
  });

  var burglaryBreakingAndEnteringCrimes = [];
  finalPayload.burglaryBreakingAndEnteringCrimes = burglaryBreakingAndEnteringCrimes;
  client.execute(query, ['Burglary/Breaking & Entering'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.burglaryBreakingAndEnteringCrimes.push(entry);
      })
    }
  });

  var counterfeitingForgeryCrimes = [];
  finalPayload.counterfeitingForgeryCrimes = counterfeitingForgeryCrimes;
  client.execute(query, ['Counterfeiting/Forgery'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.counterfeitingForgeryCrimes.push(entry);
      })
    }
  });

  var robberyCrimes = [];
  finalPayload.robberyCrimes = robberyCrimes;
  client.execute(query, ['Robbery'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.robberyCrimes.push(entry);
      })
    }
  });

  var aggravatedAssaultCrimes = [];
  finalPayload.aggravatedAssaultCrimes = aggravatedAssaultCrimes;
  client.execute(query, ['Aggravated Assault'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.aggravatedAssaultCrimes.push(entry);
      })
    }
  });

  var prostitutionCrimes = [];
  finalPayload.prostitutionCrimes = prostitutionCrimes;
  client.execute(query, ['Prostitution'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.prostitutionCrimes.push(entry);
      })
    }
  });

  var creditCardAndAtmFraudCrimes = [];
  finalPayload.creditCardAndAtmFraudCrimes = creditCardAndAtmFraudCrimes;
  client.execute(query, ['Credit Card/Automatic Teller Fraud'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.creditCardAndAtmFraudCrimes.push(entry);
      })
    }
  });

  var theftOfMotorVehiclePartsAndAccessoriesCrimes = [];
  finalPayload.theftOfMotorVehiclePartsAndAccessoriesCrimes = theftOfMotorVehiclePartsAndAccessoriesCrimes;
  client.execute(query, ['Theft of Motor Vehicle Parts/Accessories'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.theftOfMotorVehiclePartsAndAccessoriesCrimes.push(entry);
      })
    }
  });

  var kidnappingAbductionCrimes = [];
  finalPayload.kidnappingAbductionCrimes = kidnappingAbductionCrimes;
  client.execute(query, ['Kidnaping/Abduction'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.kidnappingAbductionCrimes.push(entry);
      })
    }
  });

  var impersonationCrimes = [];
  finalPayload.impersonationCrimes = impersonationCrimes;
  client.execute(query, ['Impersonation'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.impersonationCrimes.push(entry);
      })
    }
  });

  var shopliftingCrimes = [];
  finalPayload.shopliftingCrimes = shopliftingCrimes;
  client.execute(query, ['Shoplifting'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.shopliftingCrimes.push(entry);
      })
    }
  });

  var arsonCrimes = [];
  finalPayload.arsonCrimes = arsonCrimes;
  client.execute(query, ['Arson'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.arsonCrimes.push(entry);
      })
    }
  });

  var drugNarcoticViolationCrimes = [];
  finalPayload.drugNarcoticViolationCrimes = drugNarcoticViolationCrimes;
  client.execute(query, ['Drug/Narcotic Violation '], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.drugNarcoticViolationCrimes.push(entry);
      })
    }
  });

  var motorVehicleTheftCrimes = [];
  finalPayload.motorVehicleTheftCrimes = motorVehicleTheftCrimes;
  client.execute(query, ['Motor Vehicle Theft'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.motorVehicleTheftCrimes.push(entry);
      })
    }
  });

  var motorVehicleTheftCrimes = [];
  finalPayload.motorVehicleTheftCrimes = motorVehicleTheftCrimes;
  client.execute(query, ['Motor Vehicle Theft'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.motorVehicleTheftCrimes.push(entry);
      })
    }
  });

  var allOtherLarcenyCrimes = [];
  finalPayload.allOtherLarcenyCrimes = allOtherLarcenyCrimes;
  client.execute(query, ['All Other Larceny'], function(error, data) {
    if (error) {
      console.log(error);
    } else {
      Array.from(data).forEach(function(row) {
        var entry = createJsonEntry(row);
        finalPayload.allOtherLarcenyCrimes.push(entry);
      })
    }
    res.json(req.finalPayload);
  });
});



function createJsonEntry(row) {
  var entry = {};
  entry.incident_number = row.incident_number;
  entry.crime = row.crime;
  entry.date = row.date;
  entry.intersection = row.intersection;
  entry.intersection_address = row.intersection_address;
  entry.intersection_city = row.intersection_city;
  entry.intersection_state = row.intersection_state;
  entry.approximate_time = row.time;
  return entry;
}

//API endpoint to fetch data from Police API Endpoint and inserts data into local database
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
  res.render('update', { title: 'Database Updated!', message: 'The database has been updated. :)'});
});

module.exports = app;
