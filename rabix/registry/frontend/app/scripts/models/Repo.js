"use strict";

angular.module('registryApp')
    .factory('Repo', ['Api', function (Api) {

        var self = {};

        /**
         * Get list of repos
         *
         * @params {integer} skip
         * @params {string} searchTerm
         * @returns {object} $promise
         */
        self.getRepos = function(skip, searchTerm) {

            var isSearch = !(_.isUndefined(searchTerm) || _.isEmpty(searchTerm));
            var params = {skip: skip};

            if (isSearch) {
                params.q = searchTerm;
            }

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