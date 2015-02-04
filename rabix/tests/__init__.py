
infinite_loop = {
    "tool": {
        "softwareDescription": {},
        "documentAuthor": "boysha",
        "requirements": {
            "environment": {
                "container": {
                    "type": "docker",
                    "uri": "docker:infinite_loop#latest",
                    "imageId": "e678dddee492"
                }
            },
            "resources": {},
            "platformFeatures": []
        },
        "inputs": {},
        "outputs": {},
        "adapter": {
            "baseCmd": [],
            "stdout": "output.sam",
            "args": []
        }
    }
}

infinite_read = {
    "tool": {
        "softwareDescription": {},
        "documentAuthor": "boysha",
        "requirements": {
            "environment": {
                "container": {
                    "type": "docker",
                    "uri": "docker:infinite_read#latest",
                    "imageId": "39be8b7d2a61"
                }
            },
            "resources": {},
            "platformFeatures": []
        },
        "inputs": {
            "input": {
                "required": "Yes",
                "minItems": 1,
                "maxItems": 2,
                "type": "file",
                "adapter": {
                    "streamable": "Yes"
                }
            },
        },
        "outputs": {},
        "adapter": {
            "baseCmd": [],
            "stdout": "output.sam",
            "args": []
        }
    }
}

mock_app_good_repo = {
    "tool": {
        "softwareDescription": {},
        "documentAuthor": "boysha",
        "requirements": {
            "environment": {
                "container": {
                    "type": "docker",
                    "uri": "ubuntu:14.10",
                    "imageId": "277eb4304907"
                }
            },
            "resources": {},
            "platformFeatures": []
        },
        "inputs": {},
        "outputs": {},
        "adapter": {
            "baseCmd": [],
            "stdout": "output.sam",
            "args": []
        }
    }
}

mock_app_bad_repo = {
    "tool": {
        "softwareDescription": {},
        "documentAuthor": "boysha",
        "requirements": {
            "environment": {
                "container": {
                    "type": "docker",
                    "uri": "docker://wrongrepository#latest",
                    "imageId": "wrongid"
                }
            },
            "resources": {},
            "platformFeatures": []
        },
        "inputs": {},
        "outputs": {},
        "adapter": {
            "baseCmd": [],
            "stdout": "output.sam",
            "args": []
        }
    }
}

result_parallel_workflow = {
    "output_file": {
        "path": "chr20.file_prefix.lst.\d.fa",
        "secondaryFiles": {
            "path": "chr20.file_prefix.lst.\d.fa.idx"
        },
        "metadata": {
            "file_type": "text"
        }
    },
    "index_file": {
        "path": "chr20.file_prefix.lst.\d.fa.idx",
        "metadata": {
            "file_type": "text"
        }
    }
}

result_nested_workflow = {
    "output_file": {
        "path": "result",
        "secondaryFiles": {
            "path": "result.bai"
        },
        "metadata": {
            "file_type": "text"
        }
    },
    "index_file": {
        "path": "result.bai",
        "metadata": {
            "file_type": "text"
        }
    }
}
