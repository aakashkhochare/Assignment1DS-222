# Assignment1DS-222
To compile the Map Reduce modules, navigate to code/hadoop/naivebyes and run mvn clean install. Provide the naivebayes-1.0-SNAPSHOT-jar-with-dependencies.jar
to hadoop with  mlld.assignment1.naivebayes.NBTrain as the class for Train and  mlld.assignment1.naivebayes.NBTest as the main class for Test

The command line arguments for Hadoop Train are inputFilePath outputFilePath numberofreducers

The command line arguments for Hadoop Test are inputFilePath trainCountFilePath intermediateFilePath outputFilePath numberofreducers
All the above filepaths are hdfs filepaths.


To compile the Local Train navigate code/code/local/Train/naiveBayesTrain and run mvn clean install. Run java -jar naiveBayesTrain-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 with arguments inputfilePath outputfilePath
 
 To compile the Local Test navigate code/code/local/Test/naiveBayesTest and run mvn clean install. Run java -jar naiveBayesTest-0.0.1-SNAPSHOT-jar-with-dependencies.jar
  with arguments countFilePath inputFilePath
