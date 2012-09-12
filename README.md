# HTrain

from [![Explorys][ExplorysBanner]](https://www.explorys.com)
[ExplorysBanner]: http://media.marketwire.com/attachments/201105/65384_ExplorysMedicalLogo_PMS.eps.jpg
## HTrain is a suite of HBase related utilities from Explorys 


Some highlights of HTrain either committed or coming soon are:

Sorry, no real code yet. Just made the repository, have to get stuff into a project and attach the licenses and talk to the lawyer.

1. An alternative implementation of an InputFormat to allow you to use HBase as a source for mapreduce jobs.
   * Reads directly from on disk storefiles
   * In our testing, > 3 times faster
   * Use responsibly, be aware of the operational constraints
2. A drop in in-memory replacement for HTable 
   * Isolate your unit tests from your datastore without the pain
   * Implements HTableInterface
3. MapFile.Reader improvements
   * Allows for multithreaded index lookups
   * Isolate all of the Immutable Parameters from the disk operations


### Building the project with Maven
* Check out the project.
* `git clone git@github.com:ExplorysMedical/HTrain.git`
* Edit the pom.xml to reflect the version of hadoop and hbase you are using. (Tested on CDH3U4)
* Install to your local repository from the project directory
* `mvn clean install`
* Add the following to your maven project:


```maven
<dependency>
  <groupId>com.explorys.htrain</groupId>
  <artifactId>HTrain</artifactID>
  <version>1.0-SNAPSHOT</version>
</dependency>
```