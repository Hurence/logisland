
export default ['AppSettings', '$http', '$q', '$resource', TopicsDataService];

function TopicsDataService(AppSettings, $http, $q, $resource) {

    var res = $resource(AppSettings.topics_api + '/:id', {/*id: '@id'*/}, {
        'get':    {method: 'GET',    cache: false, isArray: false},
        'save':   {method: 'POST',   cache: false, isArray: false},
        'update': {method: 'PUT',    cache: false, isArray: false},
        'query':  {method: 'GET',    cache: false, isArray: true},
        'remove': {method: 'DELETE', cache: false},
        'delete': {method: 'DELETE', cache: false}
    });

    return res;

}

