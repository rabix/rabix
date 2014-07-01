"use strict";

angular.module('registryApp')
    .factory('Build', ['Api', function (Api) {

        var self = {};

        /**
         * Get list of builds
         *
         * @params {integer} skip
         * @returns {object} $promise
         */
        self.getBuilds = function(skip, repo) {

//            var params = {skip: skip};
//
//            if (!_.isUndefined(repo)) {
//                params.field_repo = repo.replace(/&/g, '/');
//            }
//
//            var promise = Api.builds.get(params).$promise;
//
//            return promise;

        };

        /**
         * Get build by id
         *
         * @param id
         * @returns {object} $promise
         */
        self.getBuild = function(id) {

//            var promise = Api.builds.get({id: id}).$promise;
//
//            return promise;

        };

        return self;

    }]);