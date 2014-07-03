"use strict";

angular.module('registryApp')
    .factory('Repo', ['Api', '$q', function (Api, $q) {

        var self = {};

        /**
         * Get list of repos
         *
         * @params {integer} skip
         * @returns {object} $promise
         */
        self.getRepos = function(skip) {

            var params = {skip: skip};

            var promise = Api.repos.get(params).$promise;

            return promise;

        };

        /**
         * Get repo by id
         *
         * @param id
         * @returns {object} $promise
         */
        self.getRepo = function(id) {

            var params = id.split('/');
            var owner = params[0];
            var name = params[1];

            var promise = Api.repos.get({owner: owner, name: name}).$promise;

            return promise;

        };

        return self;

    }]);