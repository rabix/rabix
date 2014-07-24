"use strict";

angular.module('registryApp')
    .service('User', ['Api', function (Api) {

        /**
         * Get user's details
         *
         * @returns {object} $promise
         */
        this.getUser = function() {

            var promise = Api.user.get().$promise;

            return promise;

        };

        /**
         * Log Out the user
         *
         * @returns {object} $promise
         */
        this.logOut = function() {

            var promise = Api.logout.confirm().$promise;

            return promise;

        };

        /**
         * Get the token of the user
         *
         * @returns {object} $promise
         */
        this.getToken = function() {

            var promise = Api.token.get().$promise;

            return promise;
        };

        /**
         * Generate the token for the user
         *
         * @returns {object} $promise
         */
        this.generateToken = function() {

            var promise = Api.token.generate().$promise;

            return promise;
        };

        /**
         * Revoke the token of the user
         *
         * @returns {object} $promise
         */
        this.revokeToken = function() {

            var promise = Api.token.revoke().$promise;

            return promise;
        };

        /**
         * Subscribe user to the mailin list
         *
         * @params {string} email
         * @returns {object} $promise
         */
        this.subscribe = function(email) {

            var promise = Api.subscribe.post({email: email}).$promise;

            return promise;
        };

        /**
         * Parse the user data
         *
         * @param result
         * @returns {object}
         */
        this.parseUser = function (result) {

            var params = ['avatar_url', 'gravatar_id', 'html_url', 'name'];
            var user = {};

            _.each(params, function (param) {
                if (angular.isDefined(result[param])) {
                    user[param] = result[param];
                }
            });

            return user;
        };

    }]);