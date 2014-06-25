'use strict';

/**
 * @ngdoc function
 * @name registryApp.controller:AboutCtrl
 * @description
 * # AboutCtrl
 * Controller of the registryApp
 */
angular.module('registryApp')
  .controller('AboutCtrl', function ($scope) {
    $scope.awesomeThings = [
      'HTML5 Boilerplate',
      'AngularJS',
      'Karma'
    ];
  });
