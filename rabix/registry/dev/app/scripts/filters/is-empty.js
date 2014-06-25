'use strict';

angular.module('registryApp')
    .filter('isEmpty', [function() {
        return function(object) {

            return _.isEmpty(object);

        };
    }]);