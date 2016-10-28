var map;
var markers = []

// var allCoordinates = ['28.704059, 77.10249', '19.075984, 72.877656','13.082680, 80.270718','40.712784, -74.005941'];

// var i = 0;

var allLocations = [];

function getDiff (a2, a1) {

  var a = [], diff = [];

    for (var i = 0; i < a1.length; i++) {
        a[a1[i]] = true;
    }

    for (var i = 0; i < a2.length; i++) {
        if (a[a2[i]]) {
            delete a[a2[i]];
        } else {
            a[a2[i]] = true;
        }
    }

    for (var k in a) {
        diff.push(k);
    }

    return diff;

}

$(document).ready(function (){


  $('.keywords').change(function() {

    var keyword = $('.keywords option:selected').data("id");
    
    i = 0;
    clearMap();
    allLocations = [];
    var identifierInterval = setInterval (function () {

      $.ajax({

      url : "search/" + keyword,
      success: function (response) {

        for (var i = response.length - 1; i >= 0; i--) {
          response[i] = response[i].toString();

        };

        var diffArray = getDiff (allLocations, response);
        allLocations =  allLocations.concat(diffArray);

        addLocations (diffArray);
        
        


      }

    });
    
    }, 5000);



    

   
    

  });


});





function initMap() {
  
  var locations = getLocations ();
  var coordinates="";
  map = new google.maps.Map(document.getElementById('map'), {
    center: new google.maps.LatLng(0, 0),
    zoom: 2
  });

  
  //addLocations (locations);


}


function getLocations () {

  return ['28.704059, 77.10249'];


}

function addLocations ( locations) {
  var geocoder = new google.maps.Geocoder();

  for (var i = 0; i < locations.length; i++) {

    geocoder.geocode( { 'address': locations[i] }, function(results, status) {
      if (status == google.maps.GeocoderStatus.OK) {

        coordinates=results[0].geometry.location;

        var marker = new google.maps.Marker({
          position: coordinates,
          map: map

        });

        markers.push(marker);
      } else {
        
        //console.log('Cannot find', locations[i]);
        //alert("Could not find location: " + locations[i]);
      }

    });

  }
}

function clearMap () {
  for (var i = 0; i < markers.length; i++) {
          markers[i].setMap(null);
  }
}

