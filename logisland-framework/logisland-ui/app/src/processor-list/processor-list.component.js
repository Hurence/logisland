
export default {
  name : 'processorList',
  config : {
    bindings         : {  processors: '<', selected : '<', addProcessor : '&onSelected' },
    templateUrl      : 'src/processor-list/processor-list.template.html'
  }
};
