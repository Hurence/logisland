
export default {
  name : 'config',
  config : {
    bindings         : { configuration: '=' },
    templateUrl      : 'src/jobs/components/details/Config.html',
    controller       : [ '$log', '$scope', '$mdDialog', ConfigController ]
  }
};

function ConfigController($log, $scope, $mdDialog)  {
    var vm = $scope;

    vm.addConfig = addConfig;
    vm.deleteConfig = deleteConfig;
    vm.showConfirm = showConfirm;

    function addConfig() {
        var index = vm.$ctrl.configuration.findIndex(function(el) {
            return el.key == vn.newKey;
        });
        if(index >= 0) {
            vm.$ctrl.configuration[index].key = vm.newKey;
            vm.$ctrl.configuration[index].type = vm.newType;
            vm.$ctrl.configuration[index].value = vm.newValue;
        }
        else {
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

    function showConfirm(ev, index) {
        // Appending dialog to document.body to cover sidenav in docs app
        var confirm = $mdDialog.confirm()
            .title('Do you confirm the deletion?')
            .textContent('This will delete the selected line.')
            .ariaLabel('Delete config line')
            .targetEvent(ev)
            .ok('Delete!')
            .cancel('Cancel');
        $mdDialog.show(confirm).then(function() {
            deleteConfig(index);
        }, function() {
        });
    };
};
