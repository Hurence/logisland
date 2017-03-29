
export default {
  name : 'navigationToolbar',
  config : {
    bindings         : { selected: '<' },
    templateUrl      : 'src/toolbars/navigation-toolbar.template.html',
    controller       : [ '$log', '$scope', NavigationToolbarController ]
  }
};

function NavigationToolbarController($log, $scope)  {
    var vm = $scope;
    var self = this;

    
};
