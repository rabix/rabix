#Rabix

## Rabix SDK

Rabix SDK is tool for easy packaging of bioinformatics tools on [docker](http://docker.com) 
images.

### rabix build

```
rabix build -c _path_to_build_file <-cwd _path_to_working_dir>
```

Build command is used for building docker images with packaged tools and wrappers.
It goes through steps defined in build file and execute them.

### Build file

Build file is file in yaml format that contains description how to build docker image. 
It is divided into steps. Steps are shown in form of list in build file.

```
steps:
  - step_1
    type: run
    ...
  - step_2
    type: build
    ...
```

Each step starts with step name (step_1 and step_2) and can be referenced further in 
build file so that next step use image made in previous step. There are two types of 
steps, run and build. Difference between run and build step is that run step execute 
commands without creating new image while build commits container and creates new docker 
image after executing all commands.

```
steps:
  - step_1
    type: run
    from: ubuntu
    cmd:
      - cmd1 param1 param2
      - cmd2 param1  
  - step_2
    type: build
    from: ${step_1}
    cmd: cmd1 param1 param2
    entrypoint: /bin/app
```
    
Under cmd you can write single command or list of commands that will be called in docker 
container for that step. If you specify the entrypoint in step it will be set as docker 
image entrypoint if step type is build, otherwise it will be set as entrypoint for container 
while executing commands.


### rabix test
_TODO


### rabix init
_TODO