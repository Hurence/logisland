
export default ['AppSettings', '$http', '$q', '$resource', JobsDataService];

function JobsDataService(AppSettings, $http, $q, $resource) {

    var jobs = null;

    var res = $resource(AppSettings.jobs_api + '/:id/:action', {/*id: '@id', action: '@action'*/}, {
        'get':    {method: 'GET',    cache: false, isArray: false},
        'save':   {method: 'POST',   cache: false, isArray: false},
        'update': {method: 'PUT',    cache: false, isArray: false},
        'query':  {method: 'GET',    cache: false, isArray: true},
        'remove': {method: 'DELETE', cache: false},
        'delete': {method: 'DELETE', cache: false},
        'start':  {method: 'POST',   params: {id: '@id', action:'start'},    cache: false, isArray: false},
        'restart':  {method: 'POST', params: {id: '@id', action:'restart'},  cache: false, isArray: false},
        'pause':  {method: 'POST',   params: {id: '@id', action:'pause'},    cache: false, isArray: false},
        'shutdown':  {method: 'POST',params: {id: '@id', action:'shutdown'}, cache: false, isArray: false}
    });

//    var j = res.query(function(){
//        console.log("DGDGDG: was re-here: " + j.length);
//        var job = j[1];
//        console.log("DGDGDG2: " + job.id);
//        //var r = res.start({id: 'dgJob1'});
//        //console.log("DGDGDG3: " + JSON.stringify(r));
//
//        //var r = res.update({name: 'dg Job2'}, {name: 'dgJob3'});
//    });
//
    return res;

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

