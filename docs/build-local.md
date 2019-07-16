# Building with local dependencies

The YuniKorn project has four repositories three of those repositories have a dependency at the go level.
These dependencies are part of the go modules and point to the github repositories.
During development it can be required to break the dependency on the committed version from github.
This requires making changes in the module file to allow loading a local copy or a forked copy from a different repository.  

## Affected repositories
The following dependencies exist between the repositories:

| repository| depends on |
| --- | --- |
| yunikorn-core | yunikorn-scheduler-interface | 
| yunikorn-k8s-shim | yunikorn-scheduler-interface, yunikorn-core |
| yunikorn-scheduler-interface | none |
| yunikorn-web | yunikorn-core |

The `yunikorn-web` repository has no direct go dependency on the other repositories. However any change to the `yunikorn-core` webservices can affect the web interface. 

## Making local changes

To make sure that the local changes will not break other parts of the build you should run:
- A full build `make` (build target depends on the repository)
- A full unit test run `make test`

Any test failures should be fixed before proceeding.

## Updating dependencies

The simplest way is to use the `replace` directive in the module file. The `replace` directive allows you to override the import path with a new (local) path.
There is no need to change any of the imports in the source code. The change must be made in the `go.mod` file of the repository that has the dependency. 

Using `replace` to use of a forked dependency, such as:
```
replace github.com/cloudera/yunikorn-core => example.com/some/forked-yunikorn
```

There is no requirement to fork and create a new repository. If you do not have a repository you can use a local checked out copy too. 
Using `replace` to use of a local directory as a dependency:
```
replace github.com/cloudera/yunikorn-core => /User/example/local/checked-out-yunikorn
```
and for the same dependency using a relative path:
```
replace github.com/cloudera/yunikorn-core => ../checked-out-yunikorn
```
Note: if the `replace` directive is using a local filesystem path, then the target must have the `go.mod` file at that location.


Further details on the modules wiki: [When should I use the 'replace' directive?](https://github.com/golang/go/wiki/Modules#when-should-i-use-the-replace-directive).