"use strict";

angular.module('registryApp')
    .factory('App', ['Api', function (Api) {

        var self = {};

        /**
         * Get list of apps
         *
         * @params {integer} skip
         * @params {string} searchTerm
         * @returns {object} $promise
         */
        self.getApps = function(skip, searchTerm, repo) {

            var isSearch = !(_.isUndefined(searchTerm) || _.isEmpty(searchTerm));
            var params = {skip: skip};

            if (isSearch) {
                params.q = searchTerm;
            }

            if (angular.isDefined(repo)) {
                params.field_repo = repo.replace(/&/g, '/');
            }

            var promise = Api.apps.get(params).$promise;

            return promise;

        };

        /**
         * Get app by id
         *
         * @param id
         * @returns {object} $promise
         */
        self.getApp = function(id) {

            var promise = Api.apps.get({id: id}).$promise;

            return promise;

        };

        return self;

    }]);