app.controller("LineCtrl",['$scope', '$http', function($scope, $http){
	
	$scope.cfArray = ["2017Q2", "2017Q1", "2016Q4", "2016Q3", "2016Q2", "2016Q1"];
	$scope.topOrigins = [
		{
			name: "LAX",
			values: []
		},
		{
			name: "SFO",
			values: []
		},
		{
			name: "BOS",
			values: []
		},
		{
			name: "ORD",
			values: []
		},
		{
			name: "SEA",
			values: []
		},
	]
	
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
	$scope.myJson = {  
		type : 'line' , 
		legend:{
		
		},
		"scale-x":{  
			"values":["2016Q1","2016Q2","2016Q3","2016Q4","2017Q1",  
				"2017Q2"],  
		 }, 
		series : [  
			{ text : "abc",
			values : [54 , 23 , 34 , 23 , 43, 40 ] 
			},  
			{ text : "def",
			values : [ 10 , 15 , 16 , 20 , 40, 35 ] }  
		]  
	};
}]);