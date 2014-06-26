'use strict';

angular.module('registryApp')
    .directive('header', ['$templateCache', '$timeout', '$route', '$modal', 'Model', function ($templateCache, $timeout, $route, $modal, Model) {
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

                        scope.view.processing = false;
                        scope.view.showOptions = false;

                        $modal.open({
                            template: $templateCache.get('views/partials/token-regenerated.html'),
                            controller: 'ModalCtrl',
                            resolve: {
                                data: function () { return {token: result.token}; }
                            }
                        });

                    });
                };

                /**
                 * Revoke the token of the user
                 */
                scope.revokeToken = function() {
                    scope.view.processing = true;
                    Model.revokeToken().then(function() {

                        scope.view.processing = false;
                        scope.view.showOptions = false;

                        $modal.open({
                            template: $templateCache.get('views/partials/token-revoked.html'),
                            controller: 'ModalCtrl',
                            resolve: { data: function () { return {}; } }
                        });

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