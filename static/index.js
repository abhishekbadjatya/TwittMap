var map;
var markers = []
var locations = [];
var currentRequest;
var index=0;
var timerLoop;


function parseResponse (resp) {

   for (var i = 0 ;  i < resp.length ; i++) {
          resp[i] = resp[i].toString();
    };

    return resp;
}
$(document).ready(function (){

  $('.key-words').change(function() {

    newChangeOfMap(null);


  });
  
});


function newChangeOfMap (object) {

    console.log(object);
    var keyword = $('.key-words option:selected').val();


    var url  = "search/"+keyword;
    if (object) {
      url = 'searchLocal/'+ keyword + '?lat=' + object.lat + '&lon=' + object.lng;
    }
    clearTheMap();

    if (currentRequest) {

      currentRequest.abort();

    }


    clearInterval (timerLoop);


    timerLoop = setInterval (function () {

    currentRequest =  $.ajax({

      url : url,
      success: function (resp) {


        resp = parseResponse (resp);

        var difference = differenceBetweenTwoArrays (locations, resp);
        locations =  locations.concat(difference);
        plotNewLocationsOnMap(locations);
      }

    });
    
    }, 1000);
  


}




function initializeTheMap() {
  
  var coordinates="";
  map = new google.maps.Map(document.getElementById('map'), {
    center: new google.maps.LatLng(0, 0),
    zoom: 2
  });

  new google.maps.event.addListener(map, 'click', function(event) {

    //marker = new google.maps.Marker({position: event.latLng, map: map});
    var lat = event.latLng.lat();
    var lng = event.latLng.lng();

    newChangeOfMap ({lat : lat, lng : lng });
  
  });

}

function plotNewLocationsOnMap ( locations ) {

  for ( ; index < locations.length; index++) {
    
        var lat=parseFloat(locations[index].split(",")[0]);
        var lng=parseFloat(locations[index].split(",")[1]);

        var myLatLng = new google.maps.LatLng(lat,lng);
        var marker = new google.maps.Marker({
          position: myLatLng,
          map: map

        });

        markers.push(marker);
  }
}

function clearTheMap () {
  locations = [];
  index=0;
  for (var i = 0; i < markers.length; i++) {
          markers[i].setMap(null);
  }

}


function differenceBetweenTwoArrays (a2, a1) {

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

