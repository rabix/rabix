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
                $rootScope.$broadcast('httpError', 'An error occurred while attempting to send request to ' + rejection.config.url);
                return $q.reject(rejection);
            },

            'response': function(response) {
                // intercept response success
                return response || $q.when(response);
            },

            'responseError': function(rejection) {
                // intercept response error
                var message = (_.isEmpty(rejection.data.message)) ? 'An error occurred while attempting to retrieve response from ' + rejection.config.url : rejection.data.message;
                $rootScope.$broadcast('httpError', message);
                return $q.reject(rejection);
            }

        };


    }]);