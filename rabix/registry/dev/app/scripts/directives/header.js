'use strict';

angular.module('registryApp')
    .directive('header', ['$templateCache', '$timeout', 'Model', function ($templateCache, $timeout, Model) {
        return {
            restrict: 'E',
            replace: true,
            template: $templateCache.get('views/partials/header.html'),
            scope: {},
            link: function (scope) {

                scope.view = {};
                scope.view.loading = true;
                scope.view.loggingIn = false;
                scope.view.processing = false;
                scope.view.showOptions = false;

                Model.getUser().then(function(result) {
                    console.log(result);
                    scope.view.user = (_.isUndefined(result.user)) ? {} : result.user;
                    scope.view.loading = false;
                });

                /**
                 * Log In the user
                 */
                scope.logIn = function() {
                    scope.view.loggingIn = true;
                    /*
                    Model.logIn().then(function(result) {
                        console.log(result);
                        scope.view.logging = false;
                    });
                    */
                    $timeout(function() {
                        scope.view.user = {username: 'test user'};
                        scope.view.loggingIn = false;
                    }, 2000)
                };

                /**
                 * Generate the token for the user
                 */
                scope.generateToken = function() {
                    scope.view.processing = true;
                    Model.generateToken().then(function(result) {
                        console.log(result);
                        scope.view.processing = false;
                    });
                };

                /**
                 * Revoke the token of the user
                 */
                scope.revokeToken = function() {
                    scope.view.processing = true;
                    Model.revokeToken().then(function(result) {
                        console.log(result);
                        scope.view.processing = false;
                    });
                };

                /**
                 * Log Out the user
                 */
                scope.logOut = function() {
                    scope.view.processing = true;
                    /*
                    Model.logOut().then(function(result) {
                        scope.view.showOptions = false;
                    });
                    */
                    $timeout(function() {
                        scope.view.user = {};
                        scope.view.showOptions = false;
                        scope.view.processing = false;
                    }, 2000)
                };


            }
        };
    }]);