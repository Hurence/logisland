
export default {
  name : 'headerToolbar',
  config : {
    bindings         : { selected: '<' },
    templateUrl      : 'src/toolbars/header-toolbar.template.html',
    controller       : [ '$log', '$scope', HeaderToolbarController ]
  }
};

function HeaderToolbarController($log, $scope)  {
    var vm = $scope;
    var self = this;

    
};
