
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

    vm.booleanOptions = [
        {value: true, text: 'true'},
        {value: false, text: 'false'}
    ];

    vm.typeOptions = [
        {value: 'STRING', text: 'string'},
        {value: 'INT', text: 'int'},
        {value: 'LONG', text: 'long'},
        {value: 'ARRAY', text: 'array'},
        {value: 'FLOAT', text: 'float'},
        {value: 'DOUBLE', text: 'double'},
        {value: 'BYTES', text: 'bytes'},
        {value: 'RECORD', text: 'record'},
        {value: 'MAP', text: 'map'},
        {value: 'ENUM', text: 'enum'},
        {value: 'BOOLEAN', text: 'boolean'},
    ];

    function addKey() {
        self.selected[self.selected.length] = {"name":"newKey","encrypted":false,"indexed":true,"persistent":true,"optional":true,"type":"STRING"};
    }

    function deleteKey(index) {
        self.selected.splice(index,1);
    }
};
