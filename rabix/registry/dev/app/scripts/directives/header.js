'use strict';

angular.module('registryApp')
    .directive('header', ['$templateCache', '$timeout', '$route', 'Model', function ($templateCache, $timeout, $route, Model) {
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

                var parseUser = function (result) {

                    var params = ['avatar_url', 'gravatar_id', 'html_url', 'name'];
                    var user = {};

                    _.each(params, function (param) {
                        if (!_.isUndefined(result[param])) {
                            user[param] = result[param];
                        }
                    });

                    return user;
                };

                Model.getUser().then(function(result) {
                    scope.view.user = parseUser(result);
                    scope.view.loading = false;
                });

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

                    Model.logOut().then(function() {
                        scope.view.user = {};
                        scope.view.showOptions = false;
                        scope.view.processing = false;
                        $route.reload();
                    });
                };


            }
        };
    }]);