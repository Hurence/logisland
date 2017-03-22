
export default {
  name : 'processorsList',
  config : {
    bindings         : {  processors: '<', selected : '<', addProcessor : '&onSelected' },
    templateUrl      : 'src/jobs/components/list/ProcessorsList.html'
  }
};
