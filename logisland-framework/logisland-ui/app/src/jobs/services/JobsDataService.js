
export default ['AppSettings', '$http', '$q', '$resource', JobsDataService];

function JobsDataService(AppSettings, $http, $q, $resource) {

    var jobs = null;

    return $resource(AppSettings.jobs_api + '/:id', {/*id: '@id'*/}, {
        'get': {method: 'GET', cache: false, isArray: false},
        'save': {method: 'POST', cache: false, isArray: false},
        'update': {method: 'PUT', cache: false, isArray: false},
        'query':  {method:'GET', cache: false, isArray: true},
        'remove':  {method:'DELETE', cache: false},
        'delete':  {method:'DELETE', cache: false}
    });

//    return {
//        loadAllJobs: function() {
//            // Simulate async nature of real remote calls
//            var deferred = $q.defer();
//            if(jobs != null) {
//                deferred.resolve(jobs);
//            }
//            else {
//                $http({
//                    method: 'GET',
//                    url: 'http://127.0.0.1:8081/jobs'
//                })
//                .then(function successCallback(response) {
//                    jobs = response.data;
//                    deferred.resolve(jobs);
//                }, function errorCallback(response) {
//                    deferred.reject("Failed to load jobs");
//                });
//            }
//            return deferred.promise;
//        }
//    };
}

