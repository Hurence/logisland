import angular from 'angular';

import App from 'src/app.module';


/**
 * Manually bootstrap the application when AngularJS and
 * the application classes have been loaded.
 */
angular
  .element( document )
  .ready( function() {
    angular
      .module( 'logisland-ui', [ App.name ] )
      // allow DI for use in controllers, unit tests
      .run(()=>{
        console.log(`Running the 'logisland-ui'`);
      });

    let body = document.getElementsByTagName("body")[0];
    angular.bootstrap( body, [ 'logisland-ui' ]);
  });
