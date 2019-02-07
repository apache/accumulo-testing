# Java API Compliance Checker Instructions

There is a tool that can analyze the difference between APIs called
[japi-compliance][japi]. This tool is useful for checking API compatability of
different Accumulo versions. To run this tool edit the xml files to specify
the location of accumulo core jars and set the library version.  Then run the
following command.
```bash
  japi-compliance-checker.pl -skip-deprecated -old japi-accumulo-1.5.xml -new japi-accumulo-1.6.xml -l accumulo
```

Optionally, you can use the --skip-classes argument with the provided exclude_classes.txt file to skip classes from
org.apache.accumulo.core.data that aren't in the public API.

This directory should have a library configuration file for each release on supported lines as well as an in-progress
for whatever version is currently the master branch. The examples below all make use of version-specific library definitions.

When looking at a patch release, you should verify that changes introduced are forwards and backwards compatible, per
semver.

  ### Backwards compatibility from x.y.z to x.y.(z+1)
  ```bash
  japi-compliance-checker.pl -old japi-accumulo-1.6.1.xml -new japi-accumulo-1.6.2.xml -l accumulo --skip-classes=exclude_classes.txt
  ```
    
  ### Forwards compatibility from x.y.z to x.y.(z+1). Note that the old / new arguments have been swapped.
  ```bash
  japi-compliance-checker.pl -new japi-accumulo-1.6.1.xml -old japi-accumulo-1.6.2.xml -l accumulo --skip-classes=exclude_classes.txt
  ```
 
When looking at a minor release, you should verify that change are backwards compatible, per semver.

  ### Backwards compatibility from x.y.z to x.(y+1).0
  ```bash
  japi-compliance-checker.pl -old japi-accumulo-1.6.1.xml -new japi-accumulo-1.7.0.xml -l accumulo --skip-classes=exclude_classes.txt
  ```

When looking at a major release, you should examine removals to make sure they are not capricious. Specifically, you should ensure that
they have been deprecated for a full major version.

  ### Advisory backwards compatibility check from x.y.z to (x+1).0.0
  ```bash
  japi-compliance-checker.pl -old japi-accumulo-1.7.0.xml -new japi-accumulo-2.0.0.xml -l accumulo --skip-classes=exclude_classes.txt
  ```


[japi]: https://lvc.github.io/japi-compliance-checker

