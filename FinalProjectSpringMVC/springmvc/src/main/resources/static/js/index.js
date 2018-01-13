	
var app = angular.module('myApp',['zingchart-angularjs']);

app.controller('LocationsController',['$scope', '$http', function($scope, $http){
	
	
	$scope.limit = 10;
	$scope.cfArray = ["2017Q2", "2017Q1", "2016Q4", "2016Q3", "2016Q2", "2016Q1"];
	
	$scope.locAvgChart = {  
		type : 'line' , 
		legend:{
		},
		"scale-x":{  
			"values":["2016Q1","2016Q2","2016Q3","2016Q4","2017Q1",  
				"2017Q2"],  
		 }, 
		series : [  
			{ 
				text : "avg",
				values : [ ] 
			}	
		]  
	};
	$scope.locMinChart = {  
			type : 'line' , 
			legend:{
			},
			"scale-x":{  
				"values":["2016Q1","2016Q2","2016Q3","2016Q4","2017Q1",  
					"2017Q2"],  
			 }, 
			series : [    
				{ 
					text : "min",
					values : [] 
				}	
			]  
		};
	$scope.locMaxChart = {  
			type : 'line' , 
			legend:{
			},
			"scale-x":{  
				"values":["2016Q1","2016Q2","2016Q3","2016Q4","2017Q1",  
					"2017Q2"],  
			 }, 
			series : [  
				{ 
					text : "max",
					values : [] 
				}
				
			]  
		};
	
	$scope.getTopLoc= function (cf) {
    	$http({
            method: 'GET',
            url:cf + '/topkLocations/part-r-00000',  
            dataType: 'json',
            headers: {
            	'Access-Control-Allow-Origin': '*'
            }
        }).success(function(data){
        	console.log(data)
            $scope.topLocations = data;
        });
    	
    	
    	$http({
            method: 'GET',
            url:cf + '/topkOrigins/part-r-00000',  
            dataType: 'json',
            headers: {
            	'Access-Control-Allow-Origin': '*'
            }
        }).success(function(data){
        	console.log(data)
            $scope.topOrigins = data; 
        });
    	
    	$http({
            method: 'GET',
            url:cf + '/topkDestinations/part-r-00000',  
            dataType: 'json',
            headers: {
            	'Access-Control-Allow-Origin': '*'
            }
        }).success(function(data){
        	console.log(data)
            $scope.topDestinations = data; 
        });
    	
    	
    	
	}
    $scope.getLocAgg = function(){
    	
    	$http({
            method: 'GET',
            url:'api/v1/getLocationAgg',
            params:{
            	"src": $scope.src,
            	"des": $scope.des
            }   
        }).success(function(data){
            $scope.locationArr = data; 
            angular.forEach($scope.locationArr, function(value, key) {
            	$scope.locAvgChart.series[0].values.unshift(value.avgFare);
            	$scope.locMinChart.series[0].values.unshift(value.minFare);
            	$scope.locMaxChart.series[0].values.unshift(value.maxFare);
            })
            console.log($scope.locChart);	
    		            
        });
    } 
    
    
}]);

app.controller('CarriersController',['$scope', '$http', function($scope, $http){
	$scope.limit = 10;
	$scope.cfArray = ["2017Q2", "2017Q1", "2016Q4", "2016Q3", "2016Q2", "2016Q1"];
	
	$scope.carAvgChart = {  
			type : 'line' , 
			legend:{
			},
			"scale-x":{  
				"values":["2016Q1","2016Q2","2016Q3","2016Q4","2017Q1",  
					"2017Q2"],  
			 }, 
			series : [  
				{ 
					text : "avg",
					values : [ ] 
				}	
			]  
		};
		$scope.carMinChart = {  
				type : 'line' , 
				legend:{
				},
				"scale-x":{  
					"values":["2016Q1","2016Q2","2016Q3","2016Q4","2017Q1",  
						"2017Q2"],  
				 }, 
				series : [    
					{ 
						text : "min",
						values : [] 
					}	
				]  
			};
		$scope.carMaxChart = {  
				type : 'line' , 
				legend:{
				},
				"scale-x":{  
					"values":["2016Q1","2016Q2","2016Q3","2016Q4","2017Q1",  
						"2017Q2"],  
				 }, 
				series : [  
					{ 
						text : "max",
						values : [] 
					}
					
				]  
			};
		$scope.carSumChart = {  
				type : 'line' , 
				legend:{
				},
				"scale-x":{  
					"values":["2016Q1","2016Q2","2016Q3","2016Q4","2017Q1",  
						"2017Q2"],  
				 }, 
				series : [  
					{ 
						text : "sum",
						values : [] 
					}
					
				]  
			};
		
	$scope.getTopCar = function (cf) {
    	$http({
            method: 'GET',
            url:cf + '/topkOpCarrier/part-r-00000',  
            dataType: 'json',
            headers: {
            	'Access-Control-Allow-Origin': '*'
            }
        }).success(function(data){
        	console.log(data)
            $scope.topOpCar = data; 
        });
    	
    	
    	$http({
            method: 'GET',
            url:cf + '/topkTktCarrier/part-r-00000',  
            dataType: 'json',
            headers: {
            	'Access-Control-Allow-Origin': '*'
            }
        }).success(function(data){
        	console.log(data)
            $scope.topTktCar = data; 
        });
    	
    	$http({
            method: 'GET',
            url:cf + '/TopRevTktCarrier/part-r-00000',  
            dataType: 'json',
            headers: {
            	'Access-Control-Allow-Origin': '*'
            }
        }).success(function(data){
        	console.log(data)
            $scope.topTktRevCar = data; 
        });
    	
	}
	$scope.getCarAgg = function(){
    	
    	$http({
            method: 'GET',
            url:'api/v1/getCarrierAgg',
            params:{
            	"cc": $scope.cc
            }   
        }).success(function(data){
        	console.log(data);
            $scope.carrierArr = data; 
            angular.forEach($scope.carrierArr, function(value, key) {
            	$scope.carAvgChart.series[0].values.unshift(value.avgFare);
            	$scope.carMinChart.series[0].values.unshift(value.minFare);
            	$scope.carMaxChart.series[0].values.unshift(value.maxFare);
            	$scope.carSumChart.series[0].values.unshift(value.sumFare);
            })
        });
    }       
	
}]);

app.controller('RoundtripController',['$scope', '$http', function($scope, $http){
	$scope.rtChart = {  
			type : 'vbar' ,
			legend:{
			},
			"scale-x":{  
				"values":["2016Q1","2016Q2","2016Q3","2016Q4","2017Q1",  
					"2017Q2"],  
			 }, 
			series : [  
				{ 
					text: "Round Trip",
					values : [] 
				},
				{ 
					text: "Single",
					values : [] 
				}
			]  
		};
	$http({
        method: 'GET',
        url:'rt.json',  
        dataType: 'json',
        headers: {
        	'Access-Control-Allow-Origin': '*'
        }
    }).success(function(data){
    	console.log(data)
        $scope.rt = data; 
    	angular.forEach($scope.rt, function(value, key) {
	    	$scope.rtChart.series[0].values.unshift(value.rt);
	    	$scope.rtChart.series[1].values.unshift(value.total - value.rt);
    	})
    	console.log($scope.rtChart);
    });
}]);
	    	

