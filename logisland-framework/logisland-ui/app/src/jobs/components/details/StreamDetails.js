
export default {
  name : 'streamDetails',
  config : {
    bindings         : {  selected: '<' },
    templateUrl      : 'src/jobs/components/details/StreamDetails.html',
    controller       : [ 'ListService', 'ProcessorsDataService', '$log', '$scope', '$rootScope', '$mdDialog', StreamDetailsController ]}
};

function StreamDetailsController(ListService, ProcessorsDataService, $log, $scope, $rootScope, $mdDialog) {
     var vm = $scope;

     vm.stdProcessors = ProcessorsDataService.getAllProcessors();
     vm.selectedStrProcessor = processors[0];
     vm.selectStdProcessor = selectStdProcessor;

     vm.toggleList = ListService.toggle;
     vm.moveProcessorUp = moveProcessorUp;
     vm.moveProcessorDown = moveProcessorDown;
     vm.editProcessor = editProcessor;
     vm.deleteProcessor = deleteProcessor;

     function selectStdProcessor(stdProcessor) {
         vm.selectedStdProcessor = stdProcessor;
         vm.$ctrl.selected.processors.push(stdProcessor);
         ListService.toggle("right_processors");
     }

     function moveProcessorUp(index) {
         var processors = $scope.$ctrl.selected.processors;
         var tmp = processors[index];
         processors[index] = processors[index-1];
         processors[index-1] = tmp;
     }

     function moveProcessorDown(index) {
         var processors = $scope.$ctrl.selected.processors;
         var tmp = processors[index];
         processors[index] = processors[index+1];
         processors[index+1] = tmp;
     }

     function editProcessor(index) {
         var processor = $scope.$ctrl.selected.processors[index];
         $rootScope.$broadcast('editProcessor', processor);
     }

     function deleteProcessor(index) {
         vm.$ctrl.selected.processors.splice(index, 1);
     }
}