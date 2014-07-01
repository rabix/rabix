'use strict';

angular.module('registryApp')
    .directive('height', [function () {
        return {
            link: function(scope, element) {

                var heading = 58;
                var height = element[0].parentNode.offsetHeight - heading;

                element[0].style.height = height + 'px';

            }
        };
    }]);