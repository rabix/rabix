"use strict";

angular.module('registryApp')
    .factory('Api', ['$resource', '$http', '$q', function ($resource, $http, $q) {

        var apiUrlRemote = 'http://5e9e1fd7.ngrok.com';
        var apiUrl = '';


        var api = {};

        api.apps = $resource(apiUrl + '/apps/:id', {id: '@id'}, {
            add: {method: 'POST'},
            update: {method: 'PUT'}
        });

        api.builds = $resource(apiUrl + '/builds/:id', {id: '@id'});

        api.log = function(range) {
            return $resource(apiUrl + '/builds/:id/:tab?json=1', {id: '@id', tab: '@tab'}, {
                get: {
                    method: 'GET',
                    headers: {'range': 'bytes=' + range + '-'},
                    transformResponse: [function(data) {
                        return { content: data };
                    }].concat($http.defaults.transformResponse)
                }
            });
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

        //api.subscribe = $resource(apiUrl + '/subscribe';

        api.subscribe = {
            post: function(email) {
                var deferred = $q.defer();
                deferred.resolve({message: 'ok', email: email});
                return {$promise: deferred.promise};
            }
        };

        return api;


    }]);