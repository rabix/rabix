"use strict";

angular.module('registryApp')
    .service('Api', ['$resource', '$http', '$q', function ($resource, $http, $q) {

        var apiUrlRemote = 'http://5e9e1fd7.ngrok.com';
        var apiUrl = '';

        this.apps = $resource(apiUrl + '/apps/:id', {id: '@id'}, {
            add: {method: 'POST'},
            update: {method: 'PUT'}
        });

        this.builds = $resource(apiUrl + '/builds/:id', {id: '@id'});

        this.log = function(range) {
            return $resource(apiUrl + '/builds/:id/:tab?json=1', {id: '@id', tab: '@tab'}, {
                get: {
                    method: 'GET',
                    headers: {'range': 'bytes=' + range + '-'},
                    transformResponse: [function(data) {
                        return { content: data };
                    }].concat($http.defaults.transformResponse)
                }
            });
        };

        this.repos = $resource(apiUrl + '/repos/:owner/:name', {owner: '@owner', name: '@name'}, {
            add: {method: 'PUT'}
        });

        // TODO remove later when /github-repos ready
        this.reposMock = {
            add: function() {
                var deferred = $q.defer();
                deferred.resolve({secred: 'blabla-bla-tra-la-bla'});
                return {$promise: deferred.promise};
            }
        };

        // TODO uncomment later when api ready
        //this.gitHubRepos = $resource(apiUrl + '/github-repos', {}, {});

        this.gitHubRepos = {
            get: function() {
                var deferred = $q.defer();

                var items = [];
                _.times(10, function (i) {
                    var added = _.random(0, 1);
                    items.push({id: 'owner/repo-'+i, 'html_url': 'http://www.github.com/repo-'+i, added: added });
                });

                deferred.resolve({items: items});
                return {$promise: deferred.promise};
            }
        };

        this.user = $resource(apiUrl + '/user');

        this.token = $resource(apiUrl + '/token', {}, {
            generate: {method: 'POST'},
            revoke: {method: 'DELETE'}
        });

        this.logout = $resource(apiUrl + '/logout', {}, {
            confirm: {method: 'POST'}
        });

        // TODO uncomment later when api ready
        //this.subscribe = $resource(apiUrl + '/subscribe';

        this.subscribe = {
            post: function(email) {
                var deferred = $q.defer();
                deferred.resolve({message: 'ok', email: email});
                return {$promise: deferred.promise};
            }
        };


    }]);