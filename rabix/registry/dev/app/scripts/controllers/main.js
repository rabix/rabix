'use strict';

/**
 * @ngdoc function
 * @name registryApp.controller:MainCtrl
 * @description
 * # MainCtrl
 * Controller of the registryApp
 */
angular.module('registryApp')
  .controller('MainCtrl', function ($scope) {
    $scope.awesomeThings = [
      'HTML5 Boilerplate',
      'AngularJS',
      'Karma'
    ];
  });
