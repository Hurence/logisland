
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
    var self = this;

    vm.addConfig = addConfig;
    vm.deleteConfig = deleteConfig;
    vm.checkKey = checkKey;
    vm.checkValue = checkValue;

    function addConfig() {
        var conf = {
            key: null,
            value: null,
            type: null
            };

        // Check if the key is already in the list
        var index = self.configuration.findIndex(function(el) {
            return el.key == vm.newKey;
        });
        if(index >= 0) {
            $log.debug("updating conf for: " + vm.newKey);
            self.configuration[index] = conf;
        }
        else {
            $log.debug("adding conf for: " + vm.newKey);
            self.configuration.push(conf);
        }
        $show();
    }

    function deleteConfig(index) {
        self.configuration.splice(index, 1);
    }

    function checkKey() {
    }

    function checkValue() {
    }
};
