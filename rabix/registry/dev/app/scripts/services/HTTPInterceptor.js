"use strict";

angular.module('registryApp')
    .factory('HTTPInterceptor', ['$q', '$rootScope', '$location', function ($q, $rootScope, $location) {

        var host = $location.protocol() + '://' + $location.host();

        return {
            'request': function(config) {
                // intercept request success
                return config || $q.when(config);
            },

            'requestError': function(rejection) {
                // intercept request error
                $rootScope.$broadcast('httpError', 'An error occurred while attempting to send request to ' + host + rejection.config.url);
                return $q.reject(rejection);
            },

            'response': function(response) {
                // intercept response success

                if (!_.isObject(response.data) && response.config.url.indexOf('.html') === -1) {
                    $rootScope.$broadcast('httpError', 'An error occurred. Check the error log from ' + host + response.config.url);
                }
                return response || $q.when(response);
            },

            'responseError': function(rejection) {
                // intercept response error
                $rootScope.$broadcast('httpError', 'An error occurred while attempting to retrieve response from ' + host + rejection.config.url);
                return $q.reject(rejection);
            }

        };


    }]);