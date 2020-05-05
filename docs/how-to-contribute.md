<!--
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 -->

# How do I contribute code?
Yunikorn uses:
* JIRA for issue tracking.
* GitHub Pull Requests to manage code review and the change itself.
* MarkDown in the source tree for the documentation.

## Find an issue
We use JIRA issues to track bugs for this project. 
Find an issue that you would like to work on, or file a new one if you have discovered a new issue.
For help with reporting issues look at the [how to report an issue](./reporting-issues.md).

The easiest way to get started working with the code base is to pick up a really easy
JIRA and work on that. This will help you get familiar with the code base, build system,
review process, etc. We flag these starter bugs [here](https://issues.apache.org/jira/issues/?jql=project%3DYUNIKORN%20AND%20status%3DOpen%20AND%20labels%3Dnewbie).

If nobody is working on an existing issue, assign it to yourself only if you intend to work on it shortly.
If you pick up an existing JIRA look for pull requests that are linked to the JIRA ticket.
Someone might be already working on it and not assigned the ticket.
JIRAs that have a pull requests linked will have the label `pull-request-available` and a link to the pull request can be found under issue links. 

For anything that is more than a trivial change, like a typo or one line code change, it’s a good idea to discuss your intended approach on the issue.
You are much more likely to have your patch reviewed and committed if you’ve already got buy-in from the YuniKorn community before you start writing the fix.

If you cannot assign the JIRA to yourself ask the community to help assign it and add you to the contributors list in JIRA.   

## Fix an issue
Fixes or improvement must be created on the `master` branch.
Fork the relevant YuniKorn project into your own project and checkout the `master` branch.
Make sure that you have an up to date code revision checked out before you start.
Create a branch to work on, a good name to use is the JIRA ID you are working on. 

Now start coding! As you are writing your patch, please keep the following things in mind:

Include tests with your patch.
If your patch adds a feature or fixes a bug and does not include tests, it will generally not be accepted.
If you are unsure how to write tests for a particular component, please ask on the JIRA for guidance.

Please keep your patch narrowly targeted to the problem described by the JIRA.
It’s better for everyone if we maintain discipline about the scope of each patch.
In general, if you find a bug while working on a specific feature, file a JIRA for the bug, check if you can assign it to yourself and fix it independently of the feature.
This helps us to differentiate between bug fixes and features and allows us to build stable maintenance releases.

Make sure you have observed the recommendations in the [coding guidelines](./coding-guidelines.md).
Before you commit you should also run the full test suite using `make test`.
Make sure that all the tests pass.

Finally, please write a good, clear commit message, with a short, descriptive title.
The descriptive title must start with the JIRA ID you are working on.
An example is: `[YUNIKORN-2] Support Gang Scheduling`
The commit message will be used to pre-fill the pull request information.
The JIRA ID in the message will automatically link the pull request and the JIRA. 
The message that follows can be used to explain what the problem was, and how it was fixed.

## Create a pull request
Please create a pull request on github with your patch.
See [opening a pull request](https://help.github.com/articles/using-pull-requests/) for all the details.

For committers: You can create a new branch, push your change and create a PR using the GitHub UI.
For contributors: you have already forked the repository and committed your changes to your fork.
Use the GitHub UI to create a PR using the `compare across forks` option.

The pull request description must include the JIRA reference that you are working on.
If you set the commit message as described above the pull request will automatically pick it up.
If you did not do that you can amend the description of the pull request to add the JIRA ID.
For example a pull request linked to [YUNIKORN-2](https://issues.apache.org/jira/browse/YUNIKORN-2) should have a description like:
`[YUNIKORN-2] Support Gang Scheduling`

## Committing a change
When a change is approved it will be committed to the `master` branch of the repository.
The commit message must include the JIRA in the first line and should include a `Closes #1` as a 
A commit will automatically close the PR.
The JIRA will not be closed automatically.

## Still got questions?
If you’re not sure about something, try to follow the style of the existing codebase.
Look at whether there are other examples in the code that do a similar thing.
Feel free to ask questions on the [dev@yunikorn.apache.org](mailto:dev@yunikorn.apache.org) list as well.

