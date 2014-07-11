'use strict';

angular.module('registryApp')
    .controller('AppsCtrl', ['$scope', '$routeParams', 'App', 'Header', 'Api', function ($scope, $routeParams, App, Header, Api) {

        Header.setActive('apps');

        /**
         * Callback when apps are loaded
         *
         * @param result
         */
        var appsLoaded = function(result) {

            $scope.view.paginator.prev = $scope.view.page > 1;
            $scope.view.paginator.next = ($scope.view.page * $scope.view.perPage) <= result.total;
            $scope.view.total = Math.ceil(result.total / $scope.view.perPage);

            $scope.view.apps = result.items;
            $scope.view.loading = false;
        };

        $scope.view = {};
        $scope.view.loading = true;
        $scope.view.apps = [];
        $scope.view.searchTerm = '';
        if ($routeParams.repo) {
            $scope.view.repo = $routeParams.repo.replace(/&/g, '/');
        }

        $scope.view.paginator = {
            prev: false,
            next: false
        };

        $scope.view.page = 1;
        $scope.view.perPage = 25;
        $scope.view.total = 0;

        App.getApps(0, '', $routeParams.repo).then(appsLoaded);

        /**
         * Go to the next/prev page
         *
         * @param dir
         */
        $scope.goToPage = function(dir) {

            if (!$scope.view.loading) {

                if (dir === 'prev') {
                    $scope.view.page -= 1;
                }
                if (dir === 'next') {
                    $scope.view.page += 1;
                }

                $scope.view.loading = true;
                var offset = ($scope.view.page - 1) * $scope.view.perPage;

                App.getApps(offset, $scope.view.searchTerm, $routeParams.repo).then(appsLoaded);

            }
        };

        /**
         * Search apps by the term
         */
        $scope.searchApps = function() {

            $scope.view.page = 1;
            App.getApps(0, $scope.view.searchTerm, $routeParams.repo).then(appsLoaded);

        };

        /**
         * Reset the search
         */
        $scope.resetSearch = function() {

            $scope.view.page = 1;
            $scope.view.searchTerm = '';
            App.getApps(0, '', $routeParams.repo).then(appsLoaded);

        };

        // TODO to be removed
        $scope.addApp = function () {
            console.log('addApp');

            var params = {
                name: 'Illumina Export to FASTQ ',
                repo: 'ntijanic/ported',
                toolkit_name: 'sbg convert',
                toolkit_version: '1.0.14',
                description: 'This tool converts sequence read files from the Illumina Export/CASAVA format to the more commonly used FASTQ format. It can also convert the Illumina quality score encoding to standard Sanger encoding.',
                app: {
                    $$type: 'app/tool/docker',
                    docker_image_ref: {
                        image_repo: 'images.sbgenomics.com/sevenbridges/sbg_sbgtools',
                        image_tag: 'de055cd962cf89d3'
                    },
                    schema: {
                        $$type: 'schema/app/sbgsdk',
                        inputs: [
                            {
                                description: 'An Illumina Export read file to be converted to FASTQ',
                                id: 'export_read',
                                list: false,
                                name: 'Illumina Export File',
                                required: false,
                                types: ['illumina_export']
                            }
                        ],
                        outputs: [
                            {
                                description: 'Input file converted to FASTQ format',
                                id: 'fastq',
                                list: false,
                                name: 'FASTQ',
                                required: false,
                                types: ['fastq']
                            }
                        ],
                        params: [
                            {
                                id: 'to_sanger',
                                description: 'Converts qualities from Illumina 1.3+ (Phred +64) to Sanger (Phred +33) encoding. [default: true]',
                                list: false,
                                name: 'Use Sanger qualities',
                                required: false,
                                type: 'boolean'
                            }
                        ]
                    },
                    wrapper_id: 'sbg_sbgtools.ExportToFastq'
                },
                app_checksum: 'sha1$405c6c797921ddea7ad59a6900bb2da58ff7fb0c',
                id: 'c3e4c2e7-3484-41df-96fe-acaf54e612b2',
                links: {
                    app_ref: 'http://5e9e1fd7.ngrok.com/apps/c3e4c2e7-3484-41df-96fe-acaf54e612b2?json#app',
                    html: 'http://5e9e1fd7.ngrok.com/apps/c3e4c2e7-3484-41df-96fe-acaf54e612b2',
                    self: 'http://5e9e1fd7.ngrok.com/apps/c3e4c2e7-3484-41df-96fe-acaf54e612b2?json'
                }
            };

            Api.apps.add(params, function (result) {
                console.log(result);
            });
        };

    }]);
