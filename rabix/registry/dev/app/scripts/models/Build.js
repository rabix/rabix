"use strict";

angular.module('registryApp')
    .factory('Build', ['Api', '$q', function (Api, $q) {

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
//            if (angular.isDefined(repo)) {
//                params.field_repo = repo.replace(/&/g, '/');
//            }
//
//            var promise = Api.builds.get(params).$promise;
//
//            return promise;

            var deferred = $q.defer();
            var builds = [];
            var total = 100;
            var statuses = ['running', 'failed', 'success'];

            _.times(total, function(i) {

                var status = statuses[_.random(0, 2)];

                var build = {
                    id: i,
                    message: 'Build message No. ' + i,
                    commit: i + 'fy890123',
                    committer: 'Komit Komitovic',
                    duration: '2 min 55 sec',
                    finished: '2 days ago',
                    status: status,
                    branch: 'master'
                };

                builds.push(build);

            });

            deferred.resolve({items: builds.slice(skip, skip + 25), total: total});

            return deferred.promise;

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

            var deferred = $q.defer();

            var statuses = ['running', 'failed', 'success'];
            var status = statuses[_.random(0, 2)];

            var build = {
                id: 1,
                message: 'Build message No. neki',
                commit: 'fy890123',
                committer: 'Komit Komitovic',
                duration: '2 min 55 sec',
                finished: '2 days ago',
                status: status,
                branch: 'master'
            };

            deferred.resolve(build);

            return deferred.promise;

        };

        self.getLog = function(skip) {

            var deferred = $q.defer();
            var total = 500;
            var log = [];

            _.times(total, function(i) {

                log.push('Log ' + i);

            });

            deferred.resolve(log.slice(skip, skip + 25));

            return deferred.promise;

        };

        return self;

    }]);