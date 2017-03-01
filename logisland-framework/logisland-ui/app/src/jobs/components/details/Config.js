
export default {
  name : 'config',
  config : {
    bindings         : { configuration: '<' },
    templateUrl      : 'src/jobs/components/details/Config.html',
    controller       : [ '$http', '$log', '$scope', '$mdDialog', ConfigController ]
  }
};

function ConfigController($http, $log, $scope, $mdDialog)  {
    var vm = $scope;

    vm.addConfig = addConfig;
    vm.deleteConfig = deleteConfig;
    vm.showConfirm = showConfirm;
    vm.editRow = editRow;
    //vm.paging = paging;

    vm.table_options = {
        columns: [
            { name: 'Key', prop: 'key', sort: 'asc'},
            { name: 'Value', prop: 'value' },
            { name: 'Type', prop: 'type' }
        ],
        cellDataGetter: function() {return "DGDGDG"},
        scrollbarV: true,
        columnMode: 'force',
        paging: {
            externalPaging: false
        }
    };

    function addConfig() {
        var index = vm.$ctrl.configuration.findIndex(function(el) {
            return el.key == vm.newKey;
        });
        if(index >= 0) {
            $log.debug("updating conf for: " + vm.newKey);
            vm.$ctrl.configuration[index].key = vm.newKey;
            vm.$ctrl.configuration[index].type = vm.newType;
            vm.$ctrl.configuration[index].value = vm.newValue;
        }
        else {
            $log.debug("adding conf for: " + vm.newKey);
            vm.$ctrl.configuration.push({"key": vm.newKey, "type": vm.newType, "value": vm.newValue});
        }
    }

    function deleteConfig(index) {
        vm.$ctrl.configuration.splice(index, 1);
    }

    function editConfig(index) {
        vm.newKey = vm.$ctrl.configuration[index].key;
        vm.newType = vm.$ctrl.configuration[index].type;
        vm.newValue = vm.$ctrl.configuration[index].value;
    }

    function editRow(ev, index) {
        $log.debug("Edit row: " + index);
        $log.debug("Edit row: " + vm.$ctrl.configuration[index]);
        $mdDialog.show({
            controller: editRowController(vm.$ctrl.configuration[index]),
            templateUrl: 'src/jobs/components/details/EditRow.html',
            parent: angular.element(document.body),
            targetEvent: ev,
            clickOutsideToClose: true,
            fullscreen: vm.customFullscreen // Only for -xs, -sm breakpoints.
        })
        .then(function(answer) {
            vm.status = 'You said the information was "' + answer + '".';
        }, function() {
            vm.status = 'You cancelled the dialog.';
        });
    }

    function editRowController(row) {
        $log.debug("row: " + row);
        function done() {
            $log.debug("done editing row");
        }
    }
//
//    function paging(offset, size) {
//        $log.debug("paging: " + offset + " - " + size);
//        vm.table_options.paging.count = vm.$ctrl.configuration.length;
//        var set = vm.$ctrl.configuration.splice(offset, size);
//        if (!vm.table_data) {
//            vm.table_data = set;
//        } else {
//            // only insert items i don't already have
//            set.forEach(function(r, i) {
//              var idx = i + (offset * size);
//              vm.table_data[idx] = r;
//            });
//        }
//    }

    function showConfirm(ev, index) {
        // Appending dialog to document.body to cover sidenav in docs app
        var confirm = $mdDialog.confirm()
            .title('Do you confirm the deletion?')
            .textContent('This will delete the selected line.')
            .ariaLabel('Delete config line')
            .targetEvent(ev)
            .ok('Delete!')
            .cancel('Cancel');
        $mdDialog.show(confirm)
        .then(  function() {
                deleteConfig(index);
            },  function() {
            }
        );
    };
};
