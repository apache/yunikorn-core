# How do I contribute code?
You need to first sign and return an [Individual Contributor License Agreement (ICLA)](https://github.com/cloudera/yunikorn-core/blob/master/CLAs/Cloudera%20ICLA_25APR2018.pdf) before we can accept and redistribute your contribution. Please submit the signed file as pdf format to CLA@cloudera.com.

**IMPORTANT**: Note that you may need to discuss with your employer before signing the ICLA, because your employer may ask you to contribute under your employment role. In this case, you need to sign a [Corporate Contributor License Agreement (CCLA)](https://github.com/cloudera/yunikorn-core/blob/master/CLAs/Cloudera%20CCLA_25APR2018.pdf).

## Find
We use Github issues to track bugs for this project. Find an issue that you would like to
work on (or file one if you have discovered a new issue!). If no-one is working on it,
assign it to yourself only if you intend to work on it shortly.

It’s a good idea to discuss your intended approach on the issue. You are much more
likely to have your patch reviewed and committed if you’ve already got buy-in from the
yunikorn community before you start.

## Fix
Now start coding! As you are writing your patch, please keep the following things in mind:

First, please include tests with your patch. If your patch adds a feature or fixes a bug
and does not include tests, it will generally not be accepted. If you are unsure how to
write tests for a particular component, please ask on the issue for guidance.

Second, please keep your patch narrowly targeted to the problem described by the issue.
It’s better for everyone if we maintain discipline about the scope of each patch. In
general, if you find a bug while working on a specific feature, file a issue for the bug,
check if you can assign it to yourself and fix it independently of the feature. This helps
us to differentiate between bug fixes and features and allows us to build stable
maintenance releases.

Finally, please write a good, clear commit message, with a short, descriptive title and
a message that is exactly long enough to explain what the problem was, and how it was
fixed.

Please create a pull request on github with your patch.
