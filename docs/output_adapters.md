## Constructing outputs of CommandLineTools

Each CommandLineTool output comes with an adapter structure which specifies how to construct the json for that output. 
Outputs can currently only be files or file lists. 

Output file metadata is constructed in the following way:
 - If "metadata" object contains "__inherit__" key, the value should specify an input id,
 and intersection of metadata of all files on that input becomes files metadata for that output.
 - Metadata (above) is overridden with other fields from adapter.metadata, if there are any.
 - if values of above adapter.metadata fields are expressions, 
 they are first evaluated with $self variable set to the output file path (that was matched by glob expression).

Accompanying files (such as indices) are specified using secondaryFiles field. 
The value is an array of suffixes that, when concatenated with file names, yield paths to index files. 
Alternatively, secondaryFiles can be an expression that yields a list of secondary files when evaluated.
