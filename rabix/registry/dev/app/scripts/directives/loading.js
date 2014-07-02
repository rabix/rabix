'use strict';

angular.module('registryApp')
    .directive('loading', ['$timeout', function ($timeout) {
        return {
            link: function(scope) {

                var timeoutId;

                scope.view.loadingDelayed = true;
                scope.view.loadingFaded = true;

                scope.$watch('view.loading', function(newVal, oldVal) {
                    if (newVal !== oldVal) {

                        scope.stopLoadingDelay();

                        scope.view.loadingFaded = false;

                        timeoutId = $timeout(function() {
                            scope.view.loadingDelayed = false;
                        }, 300);
                    }
                });

                scope.stopLoadingDelay = function() {
                    if (angular.isDefined(timeoutId)) {
                        $timeout.cancel(timeoutId);
                        timeoutId = undefined;
                    }
                };

                scope.$on('$destroy', function() {
                    scope.stopLoadingDelay();
                });

            }
        };
    }]);