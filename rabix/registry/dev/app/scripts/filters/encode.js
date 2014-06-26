'use strict';

angular.module('registryApp')
    .filter('encode', [function() {
        return function(string) {

            return string.replace(/\//g, '&');;

        };
    }]);