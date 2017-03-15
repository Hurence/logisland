
export default {
  name : 'keyTypesList',
  config : {
    bindings         : { selected: '<' },
    templateUrl      : 'src/topics/components/list/KeyTypesList.html',
    controller       : [ '$log', '$scope', KeyTypesListController ]
  }
};

function KeyTypesListController($log, $scope)  {
    var vm = $scope;
    var self = this;

    vm.addKey = addKey;
    vm.deleteKey = deleteKey;

    function addKey() {
        $log.debug("DGDGDG: " + JSON.stringify(self.selected));
        self.selected[self.selected.length] = {"name":"newKey","encrypted":false,"indexed":true,"persistent":true,"optional":true,"type":"STRING"};
    }

    function deleteKey(index) {
        $log.debug("Delete: " + index);
        self.selected.splice(index,1);
    }
};
