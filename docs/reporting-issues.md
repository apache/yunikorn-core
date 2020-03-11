# Reporting Issues in Apache YuniKorn

## For YuniKorn Users
If you have an issue with YuniKorn operation, please follow these guidelines:

If you are having an issue with setup, configuration, or some other form of behavior not matching your expectation, join the user mailing list and ask your questions in that forum.
See the [YuniKorn web page](https://yunikorn.apache.org) for information on mailing lists.
You can also ask the YuniKorn slack channel for help, check the web page for details on how to join.
If you have a bug that needs a fix in the code or in the documentation, please follow the procedure in [Filing a JIRA](#Filing-a-JIRA-for-YuniKorn-issues) below.

## For YuniKorn Developers
JIRA is used by the Apache YuniKorn project to track all issues.
These include:
1. Add new features
1. Improving existing features
1. Report bugs that need to be fixed in the codebase

If you are interested in tracking development issues in JIRA, you can browse
this [link](https://issues.apache.org/jira/projects/YUNIKORN).

## Filing a JIRA for YuniKorn issues
Go to the Apache JIRA page to file your issue.

Make sure the Project is set to YuniKorn. Set the issue type field appropriately based on your analysis or request:
* Bug
* New Feature
* Improvement
* Test
* Wish
* Task

For Summary, please provide a detailed title e.g. _K8 pod not scheduled in an empty cluster_ instead of just _YuniKorn scheduling failed_.

Please set the component field if you have isolated the issue to a particular component:

| Component | Description |
|-----------|-------------|
| build| Project build, build scripts, and git issues|
| core - cache | Core scheduler cache |
| core - common | Common code, like resources, for the core scheduler|
| core - scheduler | Core scheduling issues |
| documentation | Documentation fixes and enhancements |
| scheduler-interface | Scheduler interface specification |
| security | Security related issues |
| shim - kubernetes | K8shim issues |
| shim - yarn | Hadoop YARN shim issues |
| test - smoke | Smoke test failures |
| test - unit | Unit test failures |
| webapp | Web UI for the scheduler |

The Affects Versions/s field can be set to the earliest version of YuniKorn where you have seen the bug.
If you are not sure then just leave it empty.

If you are a developer intending to fix the bug, put your JIRA ID in the Assignee field.
Note that you need to be in the contributors list of Apache YuniKorn in order to be able to be assign a JIRA ticket.
If you have not been added to the list, send an email to the [dev@yunikorn.apache.org](mailto:dev@yunikorn.apache.org) mailing list or ask in a comment of the jira to request for it.

Please put as much detail as possible in the Description field.
Include your configuration changes, cluster size, and YuniKorn version.
Any related code or configuration that helps replicate the issue you should also add.

For bug reports: a short reproduction of the problem would be more than welcomed.
If you have logs, the specific part of the log with the error message or stack trace.
Attaching the entire log can be useful.
If you have already tried to debug the issue describe the steps you have already done.
Even if that result was that you were not able to reproduce the issue.

For new feature requests, it may include a design document.
If you do not have that or it is just a generic request work with us to design your feature and implement it.
