
export default [ProcessorsDataService];

function ProcessorsDataService() {

    var processors = [
            { "name": "Anonymizer1", "component": "Anonymizer", "documentation": "documentation1" },
            { "name": "ConvertFieldsType", "component": "ConvertFieldsType", "documentation": "documentation1" },
            { "name": "DebugStream", "component": "DebugStream", "documentation": "documentation1" },
            { "name": "GenerateRandomRecord", "component": "GenerateRandomRecord", "documentation": "documentation1" },
            { "name": "NormalizeFields", "component": "NormalizeFields", "documentation": "documentation1" },
            { "name": "ParseProperties", "component": "ParseProperties", "documentation": "documentation1" },
            { "name": "RemoveFields", "component": "RemoveFields", "documentation": "documentation1" },
            { "name": "SplitText", "component": "SplitText", "documentation": "documentation1" },
            { "name": "SplitTextMultiline", "component": "SplitTextMultiline", "documentation": "documentation1" }
        ];

    return {
        getAllProcessors: function() {
            return processors;
        }
    }
}


