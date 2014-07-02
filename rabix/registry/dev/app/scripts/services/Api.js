"use strict";

angular.module('registryApp')
    .factory('Api', ['$resource', '$http', function ($resource, $http) {

        var apiUrl = '';


        var api = {};

        api.apps = $resource(apiUrl + '/apps/:id', {id: '@id'}, {
            add: {method: 'POST'},
            update: {method: 'PUT'}
        });

        api.builds = $resource(apiUrl + '/builds/:id', {id: '@id'});

        api.log = function(range) {
            return $resource(apiUrl + '/builds/:id/:tab', {id: '@id', tab: '@tab'}, {
                get: {
                    method: 'GET',
                    headers: {'range': 'bytes=' + range + '-'},
                    transformResponse: [function(data) {
                        return { content: data };
                    }].concat($http.defaults.transformResponse)
                }
            })
        };

        api.repos = $resource(apiUrl + '/repos/:owner/:name', {owner: '@owner', name: '@name'});

        api.user = $resource(apiUrl + '/user');

        api.token = $resource(apiUrl + '/token', {}, {
            generate: {method: 'POST'},
            revoke: {method: 'DELETE'}
        });

        api.logout = $resource(apiUrl + '/logout', {}, {
            confirm: {method: 'POST'}
        });

        return api;


    }]);