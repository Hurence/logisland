
export default ['AppSettings', '$resource', ProcessorDataService];

function ProcessorDataService(AppSettings, $resource) {

    var processors = [];

    var res = $resource(AppSettings.plugins_api + '/', {}, {
            'query':  { method: 'GET',    cache: false, isArray: true }
        });

    res.query( function(plugins) {
        plugins.forEach( function(plugin) {
            if(plugin.type === 'processor') {
                //console.log("plugin: " + JSON.stringify(plugin));

                var configs = [];
                updateConfig(configs, plugin.properties);
                updateConfig(configs, plugin.dynamicProperties);

                var processor = {
                    name: plugin.name,
                    component: plugin.component,
                    documentation: plugin.description,
                    config: configs
                };


                processors[processors.length] = processor;
            }
        })
    });

    function updateConfig(configs, properties) {
        if(properties) {
            properties.forEach( function(p) {
                var config = {
                    key: p.name,
                    value: p.defaultValue
                };

                configs[configs.length] = config;
            });
        }
    }

    return {
        getAllProcessors: function() {
            return processors;
        }
    }
}
