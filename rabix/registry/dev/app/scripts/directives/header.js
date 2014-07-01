'use strict';

angular.module('registryApp')
    .service('Header', [function () {

        var self = {};

        self.active = 'apps';

        self.setActive = function (active) {
            self.active = active;
        };

        self.getActive = function () {
            return self.active;
        };

        return self;

    }])
    .directive('header', ['$templateCache', '$timeout', '$route', '$modal', 'User', 'Header', function ($templateCache, $timeout, $route, $modal, User, Header) {
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
                scope.view.active = Header.getActive();

                scope.HeaderService = Header;

                var parseUser = function (result) {

                    var params = ['avatar_url', 'gravatar_id', 'html_url', 'name'];
                    var user = {};

                    _.each(params, function (param) {
                        if (angular.isDefined(result[param])) {
                            user[param] = result[param];
                        }
                    });

                    return user;
                };

                User.getUser().then(function(result) {
                    scope.view.user = parseUser(result);
                    scope.view.loading = false;
                });

                /**
                 * Log Out the user
                 */
                scope.logOut = function() {
                    scope.view.processing = true;

                    User.logOut().then(function() {
                        scope.view.user = {};
                        scope.view.showOptions = false;
                        scope.view.processing = false;
                        $route.reload();
                    });
                };

                scope.showOptions = function () {

                    $modal.open({
                        template: $templateCache.get('views/partials/options.html'),
                        controller: 'OptionsCtrl',
                        windowClass: 'modal-options',
                        resolve: { data: function () { return {user: scope.view.user}; } }
                    });

                };

                scope.$watch('HeaderService.active', function (n, o) {
                    if (n !== o) {
                        scope.view.active = n;
                    }
                });

            }
        };
    }]);