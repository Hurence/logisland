
export default {
  name : 'fieldtypeList',
  config : {
    bindings         : { selected: '<' },
    templateUrl      : 'src/fieldtype-list/fieldtype-list.template.html',
    controller       : [ '$log', '$scope', FieldTypeListController ]
  }
};

function FieldTypeListController($log, $scope)  {
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
