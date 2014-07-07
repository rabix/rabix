'use strict';

angular.module('registryApp')
    .filter('trim', [function() {
        return function(string) {

            return string.trim();

        };
    }]);