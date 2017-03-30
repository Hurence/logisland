
export default ['AppSettings', '$http', '$q', '$resource', JobDataService];

function JobDataService(AppSettings, $http, $q, $resource) {

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

    return res;

}

