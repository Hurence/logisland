
export default {
  name : 'jobsList',
  config : {
    bindings         : {  jobs: '<', selected : '<', showDetails : '&onSelected' },
    templateUrl      : 'src/jobs/components/list/JobsList.html'
  }
};
