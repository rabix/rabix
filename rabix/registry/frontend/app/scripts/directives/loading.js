'use strict';

angular.module('registryApp')
    .directive('loading', ['$timeout', function ($timeout) {
        return {
            scope: {
                ngClass: '=',
                loading: '='
            },
            link: function(scope) {

                var timeoutId;

                var classes = scope.ngClass;
                classes.push('loading');
                classes.push('loading-fade');

                scope.$watch('loading', function(newVal, oldVal) {
                    if (newVal !== oldVal) {

                        scope.stopLoadingDelay();

                        _.remove(classes, function(cls) { return cls === 'loading-fade'; });
                        scope.$emit('classChange', classes);

                        timeoutId = $timeout(function() {
                            _.remove(classes, function(cls) { return cls === 'loading'; });
                            scope.$emit('classChange', classes);
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