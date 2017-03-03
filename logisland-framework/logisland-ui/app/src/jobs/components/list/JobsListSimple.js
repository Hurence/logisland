
export default {
  name : 'jobsListSimple',
  config : {
    bindings         : { jobs: '<', mode: '<', selected : '<', showDetails : '&onSelected' },
    templateUrl      : 'src/jobs/components/list/JobsListSimple.html',
    controller       : [ 'JobsDataService', '$log', '$scope', JobsListController ]
  }
};

function JobsListController(JobsDataService, $log, $scope) {
}