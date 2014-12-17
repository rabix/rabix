# Workflow Description

This document briefly describes the workflow model currently used by rabix.

It is based on the abstract model defined by [wfdesc](http://wf4ever.github.io/ro/#wfdesc).
Tool descriptions are a specific Process type and Artifacts (data that is passed between Processes) are any JSON-compatible structures.

We have been using an encoding that's easy to read/write by hand. Example:

```yaml
steps:
  - id: bwa
    app: "http://example.org/apps/bwa.json#bwa_mem"
    inputs:
      reads:
        $from: fastq_mates  # exposes as workflow input
      reference:
        $from: fasta
      gap_open_penalty: 3
    outputs:
      bam: aligned  # expose as workflow output
  - id: freebayes
    app: "http://example.org/apps/freebayes.json"
    inputs:
      reads:
        $from: bwa.bam  # data link
      reference:
        $from: fasta  # same input as for bwa
    outputs:
      variants: vcf  # expose as "vcf" output
```

Apart from the above "steps" array, workflows include the same input/output schema as CommandLineTools.
This schema can be automatically generated, so it is not required.

Each element of the "steps" array has the following fields:
- id: String - STEP_ID, unique to the workflow
- app: App - URL to CommandLineTool, ScriptTool or Workflow
- inputs: Object - object whose fields match inputs in the schema of the referenced app.
Values for these fields are either literals (2, “hello”) or objects in the form of ```{“$from”: “[<STEP_ID>.<OUTPUT_ID>] or [WORKFLOW_INPUT_ID]”}```
- outputs: Object - object whose fields match outputs defined in the schema of the referenced app and values with form of ```{“$to”: [WORKFLOW_OUTPUT_ID]”}```, which signifies that these outputs should be recorded.

Data flowing through links can be of any JSON-compatible type.
 Most commonly, these are files, which are just JSON objects of certain structure with URLs pointing to actual files and their indices.

If there are multiple incoming links to same input port (i.e. the value of $from is an array), a list of all incoming items will be automatically created.


## Parallel for-each

If a process receives a ```List<T>``` on a port which accepts ```T```, it is automatically executed for each item in the list.
Likewise for nested lists of any depth (the structure is preserved on the output).

Port nesting level is determined by checking the innermost lists first (against the JSON-Schema definition of the port).
 It will be possible to change this to outermost-first in the workflow description in the next iteration.

In the case where multiple process ports receive nested data, exception is raised.
 This will be changed to allow different strategies.


## Script Processes

For basic manipulation of (meta)data, we are using another Process type which simply evaluates an expression or script.
 This allows us to implement control structures such as GroupBy or Filter without running an external process in a container.

## Examples

See rabix/tests/wf_tests.yaml for example workflows and script tools.
