Machine learning integration design
===================================
logisland is a framework that you can extend through its API,
you can use it to build your own ``Machine Learning Jobs`` .




Design
++++++
You can use a ``Model`` to make predictions over input ``Records`` and train this ``Model`` through the
``ModelTrainingEngine``.


The model himself : Model
-------------------------
A model that can be used to make prediction on Records

.. code-block:: java

    public interface Model extends ConfigurableComponent {

        public static final AllowableValue MMLIB_KMEANS_CENTROID = new AllowableValue(
            "mllib_kmeans",
            "MLLIB KMeans centroid",
            "A centroid list for MLLIB Kmeans");


        PropertyDescriptor MODEL_TYPE = new PropertyDescriptor.Builder()
            .name("model.type")
            .description("The ML model type")
            .allowableValues(MMLIB_KMEANS_CENTROID,)
            .build();


        Record predict(Record inputRecord) throws ModelPredictionException;

        Collection<Record> predict(Collection<Record> inputRecord) throws ModelPredictionException;
    }

Train the model : ModelTrainingEngine
-------------------------------------
This component owns the training logic (with Spark MLLIB, DeepLearning4j, or What ever) of the model

.. code-block:: java

    public interface ModelTrainingEngine extends ProcessingEngine {

        PropertyDescriptor MODEL_REFRESH_DELAY = new PropertyDescriptor.Builder()
            .name("model.refresh.delay")
            .description("The delay before retraining model")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

        PropertyDescriptor DATA_LOCATION = new PropertyDescriptor.Builder()
            .name("data.location")
            .description("The place where sit the input data kafka://data/logisland_traces or " +
                "hdfs://data/logisland_traces or" +
                "http://logisland-agent:9000/data/logisland_traces or" +
                "file://data/logisland_traces ")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

        PropertyDescriptor DATA_BUCKET = new PropertyDescriptor.Builder()
            .name("data.bucket")
            .description("the period to consider, all data, between two dates/offsets")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

        PropertyDescriptor MODEL_LOCATION = new PropertyDescriptor.Builder()
            .name("model.location")
            .description("The place where sit the output model kafka://models/trace_analytics or " +
                "hdfs://data/models/trace_analytics or" +
                "http://logisland-agent:9000/models/trace_analytics or" +
                "file://data/models/trace_analytics ")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

        Model train(Model untrainedModel) throws ModelTrainingException;
    }

Access the model : ModelFactoryService
--------------------------------------
This component is used to retrieve and automatically refresh a ``Model``

.. code-block:: java

    public interface ModelFactoryService extends ControllerService {

        PropertyDescriptor VALIDITY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("validity.timeout")
            .description("model validity duration in ms. ")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


        Model get(String modelLocation) throws ModelNotFoundException;
    }


Use cases
+++++++++
Here are some example use cases described below.


Network trace clustering
------------------------







Image classification with Deep Learning
---------------------------------------

