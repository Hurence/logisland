
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
            {
                name: 'Key',
                prop: 'key',
                sort: 'asc',
            },
            {
                name: 'Value',
                prop: 'value'
            },
            {
                name: 'Type',
                prop: 'type'
            }
//            ,{
//                name: 'Action',
//                cellRenderer: function(scope, elem, row) {
//                    scope.clicker = vm.clicker;
//                    return '<a href="#" class="util-col delete-btn" ng-click="clicker($index)" style="font-size:10px;">Delete</a>';
//                }}
        ],
        scrollbarV: false,
        forceFillColumns: true,
        columnMode: 'force',
        paging: {
            externalPaging: false
        }
    };

    function addConfig() {
        var conf = {
            key: vm.newKey,
            value: vm.newValue,
            type: vm.newType
            };

        // Check if the key is already in the list
        var index = vm.$ctrl.configuration.findIndex(function(el) {
            return el.key == vm.newKey;
        });
        if(index >= 0) {
            $log.debug("updating conf for: " + vm.newKey);
            vm.$ctrl.configuration[index] = conf;
        }
        else {
            $log.debug("adding conf for: " + vm.newKey);
            vm.$ctrl.configuration.push(conf);
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

    function editRow(row, parentElement) {
        $mdDialog.show({
            controller: DialogController,
            templateUrl: 'src/jobs/components/details/EditRow.html',
            locals: {
                row: row
            },
            parent: parentElement,
            //targetEvent: ev,
            clickOutsideToClose: true,
            disableParentScroll: true,
            fullscreen: false
        })
        .then(function(editedRow) {
             row.key = editedRow.key;
             row.value = editedRow.value;
             row.type = editedRow.type;
         }, function() {
            //Cancelled
        });
    }

    function DialogController($scope, $mdDialog, row) {
        $scope.row = JSON.parse(JSON.stringify(row));
        $scope.done = function(row) {
            $mdDialog.hide($scope.row);
        }
        $scope.cancel = function() {
            $mdDialog.cancel();
        }
    }
};
