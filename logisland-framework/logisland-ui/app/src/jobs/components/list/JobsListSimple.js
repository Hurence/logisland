
export default {
  name : 'jobsListSimple',
  config : {
    bindings         : { jobs: '<', mode: '<', selected : '<', showDetails : '&onSelected' },
    templateUrl      : 'src/jobs/components/list/JobsListSimple.html',
    controller       : [ 'JobDataService', '$log', '$scope', JobsListController ]
  }
};

function JobsListController(JobDataService, $log, $scope) {
}