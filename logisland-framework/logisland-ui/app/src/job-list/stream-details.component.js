
export default {
  name : 'streamDetails',
  config : {
    bindings         : {  selected: '<' },
    templateUrl      : 'src/job-list/stream-details.template.html',
    controller       : [ 'ListService', 'ProcessorDataService', '$log', '$scope', '$rootScope', '$mdDialog', StreamDetailsController ]}
};

function StreamDetailsController(ListService, ProcessorDataService, $log, $scope, $rootScope, $mdDialog) {
    var vm = $scope;

    vm.stdProcessors = ProcessorDataService.getAllProcessors();
    vm.selectedStrProcessor = vm.processors;
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
        var processor = vm.$ctrl.selected.processors[index];
        $mdDialog.show({
            controller: EditProcessorDialogController,
            templateUrl: 'src/jobs/components/details/EditProcessor.html',
            locals: {
                processor: processor
            },
            parent: angular.element(document.body),
            //targetEvent: ev,
            clickOutsideToClose: false,
            disableParentScroll: true,
            fullscreen: false
        })
        .then(function(editedProcessor) {
            vm.$ctrl.selected.processors[index] = editedProcessor;
        }, function() {
        });
    }

    function EditProcessorDialogController($scope, $mdDialog, processor) {
        $scope.processor = JSON.parse(JSON.stringify(processor));
        $scope.done = function(processor) {
            $mdDialog.hide($scope.processor);
        }
        $scope.cancel = function() {
            $mdDialog.cancel();
        }
    }

    function deleteProcessor(index) {
        vm.$ctrl.selected.processors.splice(index, 1);
    }
}

