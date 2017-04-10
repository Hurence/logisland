
export default {
  name : 'processorList',
  config : {
    bindings         : {  processors: '<', selected : '<', addProcessor : '&onSelected' },
    templateUrl      : 'src/processor-list/processor-list.template.html',
    controller: ['ProcessorDataService', '$log', '$scope',  ProcessorListController]
  }
};



function ProcessorListController(ProcessorDataService, $log, $scope) {
    var vm = $scope;
    var self = this;


   self.processors =  ProcessorDataService.getAllProcessors();


};