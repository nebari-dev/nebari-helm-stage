# Nebari Helm Stage

**Work in-progress.**

The `NebariHelmStage` inherits from the `NebariStage` base class and is a special stage class used to bundle and appropriately sets important values (such as `domain`) that may only be known after previous stages are deployed.


The `NebariHelmStage` class can now be used to create a Helm stage with relative ease. 

See the [nebari-label-studio-chart](https://github.com/nebari-dev/nebari-label-studio-chart) as a *WIP* example.


If you have ever used Helm, you will be familiar with the functions in the `helm` module. 


