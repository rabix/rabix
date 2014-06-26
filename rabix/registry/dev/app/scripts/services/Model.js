"use strict";

angular.module('registryApp')
    .factory('Model', ['Api', function (Api) {

        var self = {};

        /**
         * Get list of apps
         *
         * @params {integer} skip
         * @params {string} searchTerm
         * @returns {object} $promise
         */
        self.getApps = function(skip, searchTerm, username, repoName) {

            var call = 'apps';
            var isSearch = !(_.isUndefined(searchTerm) || _.isEmpty(searchTerm));
            var params = {skip: skip};

            if (isSearch) {
                params.term = searchTerm;
                call = 'search';
            }

            if (!_.isUndefined(username)) {
                params.repo = username + '/' + repoName;
            }

            var promise = Api[call].get(params).$promise;

            return promise;

        };

        self.getApp = function(id) {

            var promise = Api.apps.get({id: id}).$promise;

            return promise;

        };

        /**
         * Get user's details
         *
         * @returns {object} $promise
         */
        self.getUser = function() {

            var promise = Api.user.get().$promise;

            return promise;

        };

        /**
         * Log In the user
         *
         * @returns {object} $promise
         */
        self.logIn = function() {

            var promise = Api.login.get().$promise;

            return promise;

        };

        /**
         * Log Out the user
         *
         * @returns {object} $promise
         */
        self.logOut = function() {

            var promise = Api.logout.confirm().$promise;

            return promise;

        };

        /**
         * Get the token of the user
         *
         * @returns {object} $promise
         */
        self.getToken = function() {

            var promise = Api.token.get().$promise;

            return promise;
        };

        /**
         * Generate the token for the user
         *
         * @returns {object} $promise
         */
        self.generateToken = function() {

            var promise = Api.token.generate().$promise;

            return promise;
        };

        /**
         * Revoke the token of the user
         *
         * @returns {object} $promise
         */
        self.revokeToken = function() {

            var promise = Api.token.revoke().$promise;

            return promise;
        };

        return self;


    }]);