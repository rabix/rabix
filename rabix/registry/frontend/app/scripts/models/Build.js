"use strict";

angular.module('registryApp')
    .service('Build', ['Api', '$q', function (Api, $q) {

        /**
         * Get list of builds
         *
         * @params {integer} skip
         * @returns {object} $promise
         */
        this.getBuilds = function(skip, repo) {

            var params = {skip: skip};

            if (angular.isDefined(repo)) {
                params.field_repo = repo.replace(/&/g, '/');
            }

            var promise = Api.builds.get(params).$promise;

            return promise;

        };

        /**
         * Get build by id
         *
         * @param id
         * @returns {object} $promise
         */
        this.getBuild = function(id) {

            var promise = Api.builds.get({id: id}).$promise;

            return promise;

        };

        /**
         * Get log for particular build
         * @param id
         * @param range
         * @returns {*}
         */
        this.getLog = function(id, range) {

            var deferred = $q.defer();

            Api.log(range).get({id: id, tab: 'log'}, function(result, headers) {

                deferred.resolve({
                    status: headers('X-BUILD-STATUS'),
                    contentLength: headers('Content-Length'),
                    content: result.content
                });

            });

            return deferred.promise;

        };

    }]);