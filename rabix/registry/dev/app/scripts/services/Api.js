"use strict";

angular.module('registryApp')
    .factory('Api', ['$resource', function ($resource) {

        var apiUrl = '';


        var api = {};

        api.apps = $resource(apiUrl + '/apps/:id', {app_id: '@id'}, {
            add: {method: 'POST'},
            update: {method: 'PUT'}
        });

        api.builds = $resource(apiUrl + '/builds/:id', {build_id: '@id'});

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